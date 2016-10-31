package backup.datanode;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_STORE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_STORE_KEY;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY;
import static backup.BackupConstants.LOCKS;
import static backup.BackupConstants.RESTORE;

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

import backup.BackupExtendedBlock;
import backup.store.BackupStore;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;
import backup.zookeeper.ZooKeeperLockManager;

public class DataNodeRestoreProcessor implements Runnable, Closeable {


  private final static Logger LOG = LoggerFactory.getLogger(DataNodeRestoreProcessor.class);

  private final static Map<DataNode, DataNodeRestoreProcessor> INSTANCES = new MapMaker().makeMap();

  private final DataNode datanode;
  private final Thread thread;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final BackupStore backupStore;
  private final ZooKeeperClient zooKeeper;
  private final ZooKeeperLockManager lockManager;
  private final int bytesPerChecksum;
  private final Type checksumType;
  private final long pollTime;
  private final Object lock = new Object();
  private final Watcher watch = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  };

  public static synchronized DataNodeRestoreProcessor newInstance(Configuration conf, DataNode datanode)
      throws Exception {
    DataNodeRestoreProcessor processor = INSTANCES.get(datanode);
    if (processor == null) {
      processor = new DataNodeRestoreProcessor(conf, datanode);
      INSTANCES.put(datanode, processor);
    }
    return processor;
  }

  private DataNodeRestoreProcessor(Configuration conf, DataNode datanode) throws Exception {
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
        restoreBlocks();
      } catch (Throwable t) {
        if (isRunning()) {
          LOG.error("unknown error", t);
        }
      }
    }
  }

  public void restoreBlocks() throws Exception {
    ZkUtils.mkNodesStr(zooKeeper, ZkUtils.createPath(RESTORE));
    while (isRunning()) {
      synchronized (lock) {
        List<String> requestPaths = zooKeeper.getChildren(RESTORE, watch);
        for (String requestPath : requestPaths) {
          String path = ZkUtils.createPath(RESTORE, requestPath);
          ExtendedBlock extendedBlock = getExtendedBlock(path);
          if (extendedBlock != null) {
            restoreBlock(extendedBlock);
            zooKeeper.delete(path, -1);
          }
        }
        lock.wait(pollTime);
      }
    }
  }

  private ExtendedBlock getExtendedBlock(String path) throws Exception {
    Stat stat = zooKeeper.exists(path, false);
    if (stat == null) {
      return null;
    }
    byte[] bs = zooKeeper.getData(path, false, stat);
    return BackupExtendedBlock.toBackupExtendedBlock(bs);
  }

  public void restoreBlock(ExtendedBlock extendedBlock) throws Exception {
    if (!backupStore.hasBlock(extendedBlock)) {
      LOG.error("Can not restore block, not in block store {}", extendedBlock);
      return;
    }
    FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
    if (fsDataset.isValidBlock(extendedBlock)) {
      LOG.info("Block already restored {}", extendedBlock);
      return;
    }
    String blockId = Long.toString(extendedBlock.getBlockId());
    if (lockManager.tryToLock(blockId)) {
      try {
        LOG.info("Restoring block {}", extendedBlock);
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
      } finally {
        lockManager.unlock(blockId);
      }
    }
  }

  private boolean isRunning() {
    return running.get();
  }

}
