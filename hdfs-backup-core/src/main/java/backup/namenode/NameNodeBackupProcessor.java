package backup.namenode;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY;
import static backup.BackupConstants.LOCKS;
import static backup.BackupConstants.RESTORE;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import backup.BackupExtendedBlock;
import backup.BaseProcessor;
import backup.store.ExtendedBlock;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;
import backup.zookeeper.ZooKeeperLockManager;

public class NameNodeBackupProcessor extends BaseProcessor {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupProcessor.class);

  private final NameNode namenode;
  private final ZooKeeperClient zooKeeper;
  private final ZooKeeperLockManager lockManager;
  private final long pollTime;
  private final Set<ExtendedBlock> currentRequestedRestore;
  private final NameNodeBackupBlockCheckProcessor blockCheck;

  public NameNodeBackupProcessor(Configuration conf, NameNode namenode) throws Exception {
    this.namenode = namenode;

    Cache<ExtendedBlock, Boolean> cache = CacheBuilder.newBuilder()
                                                      .expireAfterWrite(1, TimeUnit.MINUTES)
                                                      .build();
    currentRequestedRestore = Collections.newSetFromMap(cache.asMap());

    int zkSessionTimeout = conf.getInt(DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY,
        DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
    String zkConnectionString = conf.get(DFS_BACKUP_ZOOKEEPER_CONNECTION);
    pollTime = conf.getLong(DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY,
        DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT);
    if (zkConnectionString == null) {
      throw new RuntimeException("ZooKeeper connection string missing [" + DFS_BACKUP_ZOOKEEPER_CONNECTION + "].");
    }
    zooKeeper = ZkUtils.newZooKeeper(zkConnectionString, zkSessionTimeout);
    ZkUtils.mkNodesStr(zooKeeper, ZkUtils.createPath(LOCKS));
    ZkUtils.mkNodesStr(zooKeeper, ZkUtils.createPath(RESTORE));

    lockManager = new ZooKeeperLockManager(zooKeeper, ZkUtils.createPath(LOCKS));
    blockCheck = new NameNodeBackupBlockCheckProcessor(conf, zooKeeper, this);
    start();
  }

  @Override
  protected void closeInternal() {
    IOUtils.closeQuietly(blockCheck);
    IOUtils.closeQuietly(lockManager);
    IOUtils.closeQuietly(zooKeeper);
  }

  @Override
  protected void runInternal() throws Exception {
    if (!checkForBlocksToRestore()) {
      Thread.sleep(pollTime);
    }
  }

  private boolean checkForBlocksToRestore() throws Exception {
    FSNamesystem namesystem = namenode.getNamesystem();
    String blockPoolId = namesystem.getBlockPoolId();
    BlockManager blockManager = namesystem.getBlockManager();
    Iterator<? extends Block> blockIterator = blockManager.getCorruptReplicaBlockIterator();
    boolean atLeastOneRestoreRequest = false;
    while (blockIterator.hasNext()) {
      Block block = blockIterator.next();
      long blockId = block.getBlockId();
      long length = block.getNumBytes();
      long generationStamp = block.getGenerationStamp();
      ExtendedBlock extendedBlock = new ExtendedBlock(blockPoolId,blockId,length,generationStamp);
      if (!hasRestoreBeenRequested(extendedBlock)) {
        LOG.info("Need to restore block {}", extendedBlock);
        requestRestore(extendedBlock);
        atLeastOneRestoreRequest = true;
      }
    }
    return atLeastOneRestoreRequest;
  }

  public synchronized void requestRestore(ExtendedBlock extendedBlock) throws Exception {
    String path = ZkUtils.createPath(RESTORE, Long.toString(extendedBlock.getBlockId()));
    Stat stat = zooKeeper.exists(path, false);
    if (stat == null) {
      zooKeeper.create(path, BackupExtendedBlock.toBytes(extendedBlock), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    currentRequestedRestore.add(extendedBlock);
  }

  private boolean hasRestoreBeenRequested(ExtendedBlock extendedBlock) {
    return currentRequestedRestore.contains(extendedBlock);
  }

  public void runBlockCheck() throws Exception {
    this.blockCheck.runBlockCheck();
  }

}
