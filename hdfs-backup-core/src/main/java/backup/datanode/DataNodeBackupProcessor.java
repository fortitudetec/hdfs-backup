package backup.datanode;

import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_CHECK_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_CHECK_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY;
import static backup.BackupConstants.LOCKS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.BaseProcessor;
import backup.store.BackupStore;
import backup.store.ConfigurationConverter;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockConverter;
import backup.store.LengthInputStream;
import backup.store.WritableExtendedBlock;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;
import backup.zookeeper.ZooKeeperLockManager;

public class DataNodeBackupProcessor extends BaseProcessor implements BackupRPC {

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

  public DataNodeBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    this.datanode = datanode;
    pollTime = conf.getLong(DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY,
        DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT);

    checkTimeDelay = conf.getLong(DFS_BACKUP_DATANODE_CHECK_POLL_TIME_KEY, DFS_BACKUP_DATANODE_CHECK_POLL_TIME_DEFAULT);
    int threads = conf.getInt(DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_KEY,
        DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_DEFAULT);
    executorService = Executors.newFixedThreadPool(threads);
    for (int t = 0; t < threads; t++) {
      executorService.submit(getRunnableToPerformBackup());
    }
    int zkSessionTimeout = conf.getInt(DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY, 30000);
    String zkConnectionString = conf.get(DFS_BACKUP_ZOOKEEPER_CONNECTION);
    if (zkConnectionString == null) {
      throw new RuntimeException("ZooKeeper connection string missing [" + DFS_BACKUP_ZOOKEEPER_CONNECTION + "].");
    }
    zooKeeper = ZkUtils.newZooKeeper(zkConnectionString, zkSessionTimeout);
    ZkUtils.mkNodesStr(zooKeeper, ZkUtils.createPath(LOCKS));
    lockManager = new ZooKeeperLockManager(zooKeeper, ZkUtils.createPath(LOCKS));

    backupStore = BackupStore.create(ConfigurationConverter.convert(conf));
    start();
  }

  /**
   * This method can not fail or block or this could cause problems for the
   * datanode itself.
   * 
   * @param extendedBlock
   */
  public void blockFinalized(ExtendedBlock extendedBlock) {
    try {
      finializedBlocks.put(extendedBlock);
    } catch (InterruptedException e) {
      LOG.error("error adding new block to internal work queue {}", extendedBlock);
    }
  }

  @Override
  protected void closeInternal() {
    executorService.shutdownNow();
    IOUtils.closeQuietly(lockManager);
    IOUtils.closeQuietly(zooKeeper);

  }

  @Override
  protected void runInternal() throws Exception {
    if (!runFutureCheck()) {
      Thread.sleep(pollTime);
    }
  }

  /**
   * If blocks are copied to backup store return true. Otherwise return false.
   * 
   * @return
   * @throws Exception
   */
  boolean backupBlocks() throws Exception {
    ExtendedBlock extendedBlock = finializedBlocks.take();
    try {
      backupBlock(extendedBlock);
    } catch (Exception e) {
      LOG.error("Unknown error", e);
      // try again
      finializedBlocks.put(extendedBlock);
    }
    return true;
  }

  private Runnable getRunnableToPerformBackup() {
    return new Runnable() {
      @Override
      public void run() {
        while (isRunning()) {
          try {
            if (!backupBlocks()) {
              Thread.sleep(pollTime);
            }
          } catch (Throwable t) {
            if (isRunning()) {
              LOG.error("unknown error", t);
            }
          }
        }
      }
    };
  }

  public void backupBlock(ExtendedBlock extendedBlock) throws Exception {
    String blockId = Long.toString(extendedBlock.getBlockId());
    if (lockManager.tryToLock(blockId)) {
      try {
        FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
        org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = ExtendedBlockConverter.toHadoop(extendedBlock);
        BlockLocalPathInfo blockLocalPathInfo = fsDataset.getBlockLocalPathInfo(heb);
        long numBytes = blockLocalPathInfo.getNumBytes();
        try (LengthInputStream data = new LengthInputStream(fsDataset.getBlockInputStream(heb, 0), numBytes)) {
          org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream tmeta = fsDataset.getMetaDataInputStream(
              heb);
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

  private boolean runFutureCheck() throws Exception {
    FutureExtendedBlockCheck check = futureChecks.peek();
    if (check == null || !check.needsToBeChecked()) {
      return false;
    }
    List<FutureExtendedBlockCheck> checks = new ArrayList<>();
    futureChecks.drainTo(checks, maxBlocksToCheck);
    for (FutureExtendedBlockCheck futureExtendedBlockCheck : checks) {
      backupBlock(futureExtendedBlockCheck.block);
    }
    return true;
  }

  public boolean tryToBackupBlocksAgain(List<ExtendedBlock> blocks) throws Exception {
    FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
    boolean backupOccured = false;
    for (ExtendedBlock extendedBlock : blocks) {
      if (fsDataset.getVolume(ExtendedBlockConverter.toHadoop(extendedBlock)) != null) {
        // This datanode has the block
        backupBlock(extendedBlock);
        backupOccured = true;
      }
    }
    return backupOccured;
  }

  @Override
  public void backupBlock(WritableExtendedBlock extendedBlock) throws IOException {
    try {
      finializedBlocks.put(extendedBlock.getExtendedBlock());
    } catch (InterruptedException e) {
      LOG.error("error adding new block to internal work queue {}", extendedBlock);
      throw new IOException(e);
    }
  }

}
