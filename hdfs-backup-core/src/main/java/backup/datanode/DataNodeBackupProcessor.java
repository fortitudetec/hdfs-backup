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

import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION_KEY;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY;
import static backup.BackupConstants.LOCKS;

import java.io.Closeable;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.datanode.ipc.BackupStats;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.LengthInputStream;
import backup.util.Closer;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClientFactory;
import backup.zookeeper.ZooKeeperLockManager;

public class DataNodeBackupProcessor implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeBackupProcessor.class);

  private final DataNode datanode;
  private final BackupStore backupStore;
  private final ZooKeeperClientFactory zkcf;
  private final ZooKeeperLockManager lockManager;
  private final Closer closer;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicInteger backupsInProgress = new AtomicInteger();
  private final Meter bytesPerSecond = new Meter();

  public DataNodeBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    this.closer = Closer.create();
    this.datanode = datanode;
    backupStore = closer.register(BackupStore.create(BackupUtil.convert(conf)));

    int zkSessionTimeout = conf.getInt(DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY,
        DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
    String zkConnectionString = conf.get(DFS_BACKUP_ZOOKEEPER_CONNECTION_KEY);
    if (zkConnectionString == null) {
      throw new RuntimeException("ZooKeeper connection string missing [" + DFS_BACKUP_ZOOKEEPER_CONNECTION_KEY + "].");
    }
    zkcf = closer.register(ZkUtils.newZooKeeperClientFactory(zkConnectionString, zkSessionTimeout));
    lockManager = closer.register(new ZooKeeperLockManager(zkcf, LOCKS));
  }

  public BackupStats getBackupStats() {
    BackupStats backupStats = new BackupStats();
    backupStats.setBackupsInProgressCount(backupsInProgress.get());
    backupStats.setBackupBytesPerSecond(bytesPerSecond.getCountPerSecond());
    return backupStats;
  }

  /**
   * This method can not fail or block or this could cause problems for the
   * datanode itself.
   * 
   * @throws Exception
   */
  public void blockFinalized(boolean bypassLock, ExtendedBlock extendedBlock) throws Exception {
    if (backupStore.hasBlock(extendedBlock)) {
      LOG.info("block {} already backed up", extendedBlock);
      return;
    }
    String blockId = Long.toString(extendedBlock.getBlockId());
    try {
      if (bypassLock) {
        performBackup(extendedBlock, blockId);
      } else {
        try {
          if (lockManager.tryToLock(blockId)) {
            performBackup(extendedBlock, blockId);
          }
        } finally {
          backupsInProgress.decrementAndGet();
          lockManager.unlock(blockId);
        }
      }
    } catch (Exception e) {
      if (e instanceof KeeperException) {
        LOG.warn("ZooKeeper error during backup of {} bypassing lock", blockId);
        performBackup(extendedBlock, blockId);
      } else {
        throw e;
      }
    }
  }

  private void performBackup(ExtendedBlock extendedBlock, String blockId) throws Exception {
    backupsInProgress.incrementAndGet();
    FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
    org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = BackupUtil.toHadoop(extendedBlock);

    BlockLocalPathInfo blockLocalPathInfo = fsDataset.getBlockLocalPathInfo(heb);
    long numBytes = blockLocalPathInfo.getNumBytes();
    try (LengthInputStream data = new LengthInputStream(trackThroughPut(fsDataset.getBlockInputStream(heb, 0)),
        numBytes)) {
      org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream tmeta = fsDataset.getMetaDataInputStream(heb);
      try (LengthInputStream meta = new LengthInputStream(trackThroughPut(tmeta), tmeta.getLength())) {
        backupStore.backupBlock(extendedBlock, data, meta);
      }
    }
  }

  @Override
  public void close() {
    running.set(false);
    IOUtils.closeQuietly(closer);
  }

  private InputStream trackThroughPut(InputStream input) {
    return new ThroughPutInputStream(input, bytesPerSecond.getCounter());
  }

}