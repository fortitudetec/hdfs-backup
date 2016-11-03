package backup;

import java.util.concurrent.TimeUnit;

import backup.store.DevNullBackupStore;

public class BackupConstants {

  public static final String BACKUP_BLOCK_REQUESTS = "/backup-block-requests";
  public static final String LOCKS = "/locks";
  public static final String RESTORE = "/restore";

  public static final String DFS_BACKUP_STORE_KEY = "dfs.backup.store.key";
  public static final String DFS_BACKUP_STORE_DEFAULT = DevNullBackupStore.class.getName();

  public static final String DFS_DATANODE_BACKUP_FSDATASET_FACTORY_KEY = "dfs.datanode.backup.fsdataset.factory";

  public static final String DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY = "dfs.backup.zookeeper.session.timeout";
  public static final int DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 30000;

  public static final String DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY = "dfs.backup.namenode.missing.blocks.poll.time";
  public static final long DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(10);

  public static final String DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_KEY = "dfs.backup.datanode.missing.backup.store.blocks.poll.time";
  public static final long DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(
      30);

  public static final String DFS_BACKUP_DATANODE_CHECK_POLL_TIME_KEY = "dfs.backup.datanode.block.check.poll.time";
  public static final long DFS_BACKUP_DATANODE_CHECK_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(10);

  public static final String DFS_BACKUP_DATANODE_BACKUP_BLOCK_POLL_TIME_KEY = "dfs.backup.datanode.backup.block.poll.time";
  public static final long DFS_BACKUP_DATANODE_BACKUP_BLOCK_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(1);

  public static final String DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_KEY = "dfs.backup.datanode.backup.handler.count";
  public static final int DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_DEFAULT = 10;

  public static final String DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY = "dfs.backup.namenode.block.check.interval";
  public static final long DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(3);

  public static final String DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY = "dfs.backup.namenode.block.check.interval.delay";
  public static final int DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT = (int) TimeUnit.HOURS.toMillis(1);

  public static final String DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY = "dfs.backup.namenode.local.dir";
  public static final String DFS_BACKUP_NAMENODE_LOCAL_DIR_DEFAULT = "file:///hdfs/sort";

  public static final String DFS_BACKUP_ZOOKEEPER_CONNECTION = "dfs.backup.zookeeper.connection";
}
