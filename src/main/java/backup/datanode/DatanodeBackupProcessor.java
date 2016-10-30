package backup.datanode;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_STORE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_STORE_KEY;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY;
import static backup.BackupConstants.LOCKS;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

import backup.store.BackupStore;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;
import backup.zookeeper.ZooKeeperLockManager;

public class DatanodeBackupProcessor implements Runnable, Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(DatanodeBackupProcessor.class);

  private final static Map<DataNode, DatanodeBackupProcessor> INSTANCES = new MapMaker().makeMap();

  private final DataNode datanode;
  private final BlockingQueue<ExtendedBlock> finializedBlocks = new LinkedBlockingQueue<>();
  private final Thread thread;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final BackupStore backupStore;
  private final ZooKeeperClient zooKeeper;
  private final ZooKeeperLockManager lockManager;
  private final int bytesPerChecksum;
  private final Type checksumType;
  private final long pollTime;

  public static synchronized DatanodeBackupProcessor newInstance(Configuration conf, DataNode datanode)
      throws Exception {
    DatanodeBackupProcessor processor = INSTANCES.get(datanode);
    if (processor == null) {
      processor = new DatanodeBackupProcessor(conf, datanode);
      INSTANCES.put(datanode, processor);
    }
    return processor;
  }

  private DatanodeBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    this.datanode = datanode;
    pollTime = conf.getLong(DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY,
        DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT);
    this.bytesPerChecksum = conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    this.checksumType = Type
        .valueOf(conf.get(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT));
    int zkSessionTimeout = conf.getInt(DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY, 30000);
    String zkConnectionString = conf.get(DFS_BACKUP_ZOOKEEPER_CONNECTION);
    if (zkConnectionString == null) {
      throw new RuntimeException("ZooKeeper connection string missing [" + DFS_BACKUP_ZOOKEEPER_CONNECTION + "].");
    }
    zooKeeper = ZkUtils.newZooKeeper(zkConnectionString, zkSessionTimeout);
    ZkUtils.mkNodesStr(zooKeeper, ZkUtils.createPath(LOCKS));
    lockManager = new ZooKeeperLockManager(zooKeeper, ZkUtils.createPath(LOCKS));

    Class<? extends BackupStore> clazz = conf.getClass(DFS_BACKUP_STORE_KEY, DFS_BACKUP_STORE_DEFAULT,
        BackupStore.class);
    backupStore = ReflectionUtils.newInstance(clazz, conf);
    backupStore.init();

    this.thread = new Thread(this);
    thread.setDaemon(true);
    thread.setName(getClass().getName());
    thread.start();
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
  public void close() {
    IOUtils.closeQuietly(lockManager);
    IOUtils.closeQuietly(zooKeeper);
    running.set(false);
    thread.interrupt();
  }

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

  public boolean restoreBlock(ExtendedBlock extendedBlock) throws Exception {
    if (!backupStore.hasBlock(extendedBlock)) {
      return false;
    }
    FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
    if (fsDataset.isValidBlock(extendedBlock)) {
      return true;
    }
    StorageType storageType = StorageType.DEFAULT;
    boolean allowLazyPersist = true;
    ReplicaHandler replicaHandler = fsDataset.createRbw(storageType, extendedBlock, allowLazyPersist);
    ReplicaInPipelineInterface pipelineInterface = replicaHandler.getReplica();
    boolean isCreate = true;
    DataChecksum requestedChecksum = DataChecksum.newDataChecksum(checksumType, bytesPerChecksum);
    int bytesCopied = 0;
    try (ReplicaOutputStreams streams = pipelineInterface.createStreams(isCreate, requestedChecksum)) {
      try (OutputStream checksumOut = streams.getChecksumOut()) {
        try (InputStream metaData = backupStore.getMetaDataInputStream(extendedBlock)) {
          LOG.info("Restoring meta data for block {}", extendedBlock);
          IOUtils.copy(metaData, checksumOut);
        }
      }
      try (OutputStream dataOut = streams.getDataOut()) {
        try (InputStream data = backupStore.getDataInputStream(extendedBlock)) {
          LOG.info("Restoring data for block {}", extendedBlock);
          bytesCopied = IOUtils.copy(data, dataOut);
        }
      }
    }
    pipelineInterface.setNumBytes(bytesCopied);
    LOG.info("Finalizing restored block {}", extendedBlock);
    fsDataset.finalizeBlock(extendedBlock);
    datanode.notifyNamenodeReceivedBlock(extendedBlock, "", pipelineInterface.getStorageUuid());
    return true;
  }

  /**
   * If blocks are copied to backup store return true. Otherwise return false.
   * 
   * @return
   * @throws Exception
   */
  boolean backupBlocks() throws Exception {
    ExtendedBlock extendedBlock = finializedBlocks.take();
    backupBlock(extendedBlock);
    return true;
  }

  public void backupBlock(ExtendedBlock extendedBlock)
      throws KeeperException, InterruptedException, Exception, IOException {
    String blockId = Long.toString(extendedBlock.getBlockId());
    if (lockManager.tryToLock(blockId)) {
      try {
        FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
        try (InputStream data = fsDataset.getBlockInputStream(extendedBlock, 0)) {
          try (InputStream meta = fsDataset.getMetaDataInputStream(extendedBlock)) {
            backupStore.backupBlock(extendedBlock, data, meta);
          }
        }
      } finally {
        lockManager.unlock(blockId);
      }
    }
  }

  private boolean isRunning() {
    return running.get();
  }

}
