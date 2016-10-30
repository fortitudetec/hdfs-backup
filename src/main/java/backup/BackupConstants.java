package backup;

import java.util.concurrent.TimeUnit;

import backup.store.BackupStore;
import backup.store.DevNullBackupStore;

public class BackupConstants {

  public static final String LOCKS = "/locks";
  public static final String RESTORE = "/restore";

  public static final String DFS_BACKUP_STORE_KEY = "dfs.backup.store.key";
  public static final Class<? extends BackupStore> DFS_BACKUP_STORE_DEFAULT = DevNullBackupStore.class;

  public static final String DFS_DATANODE_BACKUP_FSDATASET_FACTORY_KEY = "dfs.datanode.backup.fsdataset.factory";

  public static final String DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY = "dfs.backup.zookeeper.session.timeout";
  public static final int DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 30000;

  public static final String DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY = "dfs.backup.namenode.missing.blocks.poll.time";
  public static final Long DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(10);

  public static final String DFS_BACKUP_DATANODE_BACKUP_BLOCK_POLL_TIME_KEY = "dfs.backup.datanode.backup.block.poll.time";
  public static final Long DFS_BACKUP_DATANODE_BACKUP_BLOCK_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(1);

  public static final String DFS_BACKUP_ZOOKEEPER_CONNECTION = "dfs.backup.zookeeper.connection";
}
