package backup.datanode;

import static backup.BackupConstants.BACKUP_BLOCK_REQUESTS;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_KEY;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.ImmutableSet;

import backup.BackupExtendedBlocks;
import backup.BaseProcessor;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;

public class DataNodeBackupRemoteRequestProcessor extends BaseProcessor {

  private final Object lock = new Object();
  private final Watcher watch = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  };
  private final ZooKeeperClient zooKeeper;
  private final DataNodeBackupProcessor backupProcessor;
  private final long pollTime;

  public DataNodeBackupRemoteRequestProcessor(ZooKeeperClient zooKeeper, DataNodeBackupProcessor backupProcessor,
      Configuration conf) {
    this.zooKeeper = zooKeeper;
    ZkUtils.mkNodesStr(zooKeeper, BACKUP_BLOCK_REQUESTS);
    this.backupProcessor = backupProcessor;
    this.pollTime = conf.getLong(DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_KEY,
        DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_DEFAULT);
    start();
  }

  @Override
  protected void closeInternal() {

  }

  @Override
  protected void runInternal() throws Exception {
    Set<String> blockRequests = ImmutableSet.of();
    while (isRunning()) {
      synchronized (lock) {
        Set<String> currentBlockRequests = ImmutableSet.copyOf(zooKeeper.getChildren(BACKUP_BLOCK_REQUESTS, watch));
        Set<String> newBlockRequests = difference(blockRequests, currentBlockRequests);
        for (String blockRequest : newBlockRequests) {
          List<ExtendedBlock> blocks = getExtendedBlocks(blockRequest);
          if (blocks != null) {
            backupProcessor.tryToBackupBlocksAgain(blocks);
          }
        }
        blockRequests = currentBlockRequests;
        lock.wait(pollTime);
      }
    }
  }

  private Set<String> difference(Set<String> base, Set<String> set) {
    Set<String> result = new HashSet<>(set);
    result.removeAll(base);
    return ImmutableSet.copyOf(result);
  }

  private List<ExtendedBlock> getExtendedBlocks(String blockRequest) throws Exception {
    String path = ZkUtils.createPath(BACKUP_BLOCK_REQUESTS, blockRequest);
    Stat stat = zooKeeper.exists(path, false);
    if (stat != null) {
      byte[] data = zooKeeper.getData(path, false, stat);
      return BackupExtendedBlocks.toBackupExtendedBlocks(data);
    }
    return null;
  }
}
