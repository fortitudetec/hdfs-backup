/*
 * Copyright 2016 Fortitude Technologies LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backup.datanode;

import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_ERROR_PAUSE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_ERROR_PAUSE_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_CHECK_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_CHECK_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY;
import static backup.BackupConstants.LOCKS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import backup.Executable;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.LengthInputStream;
import backup.store.WritableExtendedBlock;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;
import backup.zookeeper.ZooKeeperLockManager;

public class DataNodeBackupProcessor implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeBackupProcessor.class);

  private final DataNode datanode;
  private final BlockingQueue<ExtendedBlock> finializedBlocks = new LinkedBlockingQueue<>();
  private final BackupStore backupStore;
  private final ZooKeeperClient zooKeeper;
  private final ZooKeeperLockManager lockManager;
  private final long pollTime;
  private final ExecutorService executorService;
  private final BlockingQueue<FutureExtendedBlockCheck> futureChecks = new LinkedBlockingQueue<>();
  private final long checkTimeDelay;
  private final int maxBlocksToCheck = 100;
  private final Closer closer;
  private final AtomicBoolean running = new AtomicBoolean(true);

  public DataNodeBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    this.closer = Closer.create();
    this.datanode = datanode;
    pollTime = conf.getLong(DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY,
        DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT);
    checkTimeDelay = conf.getLong(DFS_BACKUP_DATANODE_CHECK_POLL_TIME_KEY, DFS_BACKUP_DATANODE_CHECK_POLL_TIME_DEFAULT);
    int threads = conf.getInt(DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_KEY,
        DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_DEFAULT);
    long pauseOnError = conf.getLong(DFS_BACKUP_DATANODE_BACKUP_ERROR_PAUSE_KEY,
        DFS_BACKUP_DATANODE_BACKUP_ERROR_PAUSE_DEFAULT);
    executorService = Executors.newCachedThreadPool();
    closer.register((Closeable) () -> executorService.shutdownNow());
    backupStore = closer.register(BackupStore.create(BackupUtil.convert(conf)));
    executorService.submit(Executable.createDaemon(LOG, pauseOnError, running, () -> runFutureCheck()));
    for (int t = 1; t < threads; t++) {
      executorService.submit(Executable.createDaemon(LOG, pauseOnError, running, () -> backupBlocks()));
    }
    int zkSessionTimeout = conf.getInt(DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY, 30000);
    String zkConnectionString = conf.get(DFS_BACKUP_ZOOKEEPER_CONNECTION);
    if (zkConnectionString == null) {
      throw new RuntimeException("ZooKeeper connection string missing [" + DFS_BACKUP_ZOOKEEPER_CONNECTION + "].");
    }
    zooKeeper = closer.register(ZkUtils.newZooKeeper(zkConnectionString, zkSessionTimeout));
    ZkUtils.mkNodesStr(zooKeeper, ZkUtils.createPath(LOCKS));
    lockManager = closer.register(new ZooKeeperLockManager(zooKeeper, ZkUtils.createPath(LOCKS)));
  }

  /**
   * This method can not fail or block or this could cause problems for the
   * datanode itself.
   */
  public void blockFinalized(ExtendedBlock extendedBlock) {
    try {
      finializedBlocks.put(extendedBlock);
    } catch (InterruptedException e) {
      LOG.error("error adding new block to internal work queue {}", extendedBlock);
    }
  }

  @Override
  public void close() {
    running.set(false);
    IOUtils.closeQuietly(closer);
  }

  /**
   * If blocks are copied to backup store return true. Otherwise return false.
   */
  public void backupBlocks() throws Exception {
    ExtendedBlock extendedBlock = finializedBlocks.take();
    try {
      backupBlock(extendedBlock);
    } catch (Exception e) {
      LOG.error("Unknown error", e);
      // try again
      finializedBlocks.put(extendedBlock);
    }
  }

  public void backupBlock(ExtendedBlock extendedBlock) throws Exception {
    if (backupStore.hasBlock(extendedBlock)) {
      LOG.info("block {} already backed up", extendedBlock);
      return;
    }
    String blockId = Long.toString(extendedBlock.getBlockId());
    if (lockManager.tryToLock(blockId)) {
      try {
        FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
        org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = BackupUtil.toHadoop(extendedBlock);
        BlockLocalPathInfo blockLocalPathInfo = fsDataset.getBlockLocalPathInfo(heb);
        long numBytes = blockLocalPathInfo.getNumBytes();
        try (LengthInputStream data = new LengthInputStream(fsDataset.getBlockInputStream(heb, 0), numBytes)) {
          org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream tmeta = fsDataset
              .getMetaDataInputStream(heb);
          try (LengthInputStream meta = new LengthInputStream(tmeta, tmeta.getLength())) {
            backupStore.backupBlock(extendedBlock, data, meta);
          }
        }
      } finally {
        lockManager.unlock(blockId);
      }
    } else {
      // Another data node is likely copying block, but we should check block in
      // the near future.
      addToFutureChecks(extendedBlock);
    }
  }

  private void addToFutureChecks(ExtendedBlock extendedBlock) {
    futureChecks.add(new FutureExtendedBlockCheck(getFutureCheckTime(), extendedBlock));
  }

  private long getFutureCheckTime() {
    return System.currentTimeMillis() + checkTimeDelay;
  }

  private void runFutureCheck() throws Exception {
    FutureExtendedBlockCheck check = futureChecks.peek();
    if (check == null || !check.needsToBeChecked()) {
      Thread.sleep(pollTime);
      return;
    }
    List<FutureExtendedBlockCheck> checks = new ArrayList<>();
    futureChecks.drainTo(checks, maxBlocksToCheck);
    for (FutureExtendedBlockCheck futureExtendedBlockCheck : checks) {
      backupBlock(futureExtendedBlockCheck.block);
    }
  }

  public boolean tryToBackupBlocksAgain(List<ExtendedBlock> blocks) throws Exception {
    FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
    boolean backupOccured = false;
    for (ExtendedBlock extendedBlock : blocks) {
      if (fsDataset.getVolume(BackupUtil.toHadoop(extendedBlock)) != null) {
        // This datanode has the block
        backupBlock(extendedBlock);
        backupOccured = true;
      }
    }
    return backupOccured;
  }

  public void addToBackupQueue(WritableExtendedBlock extendedBlock) throws IOException {
    try {
      finializedBlocks.put(extendedBlock.getExtendedBlock());
    } catch (InterruptedException e) {
      LOG.error("error adding new block to internal work queue {}", extendedBlock);
      throw new IOException(e);
    }
  }

  static class FutureExtendedBlockCheck {
    final long checkTime;
    final ExtendedBlock block;

    FutureExtendedBlockCheck(long checkTime, ExtendedBlock block) {
      this.checkTime = checkTime;
      this.block = block;
    }

    boolean needsToBeChecked() {
      return System.currentTimeMillis() >= checkTime;
    }
  }
}
