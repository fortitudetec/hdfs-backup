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
package backup;

import java.util.concurrent.TimeUnit;

import backup.store.DevNullBackupStore;

public class BackupConstants {

  public static final String LOCKS = "/locks";

  public static final String DFS_BACKUP_STORE_KEY = "dfs.backup.store.key";
  public static final String DFS_BACKUP_STORE_DEFAULT = DevNullBackupStore.class.getName();

  public static final String DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_KEY = "dfs.backup.zookeeper.session.timeout";
  public static final int DFS_BACKUP_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 30000;

  public static final String DFS_BACKUP_REMOTE_BACKUP_BATCH_KEY = "dfs.backup.remote.backup.batch";
  public static final int DFS_BACKUP_REMOTE_BACKUP_BATCH_DEFAULT = 100;

  public static final String DFS_BACKUP_ZOOKEEPER_CONNECTION = "dfs.backup.zookeeper.connection";

  public static final String DFS_DATANODE_BACKUP_FSDATASET_FACTORY_KEY = "dfs.datanode.backup.fsdataset.factory";

  public static final String DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_KEY = "dfs.backup.datanode.missing.backup.store.blocks.poll.time";
  public static final long DFS_BACKUP_DATANODE_MISSING_BACKUP_STORE_BLOCKS_POLL_TIME_DEFAULT = TimeUnit.SECONDS
      .toMillis(30);

  public static final String DFS_BACKUP_DATANODE_CHECK_POLL_TIME_KEY = "dfs.backup.datanode.block.check.poll.time";
  public static final long DFS_BACKUP_DATANODE_CHECK_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(10);

  public static final String DFS_BACKUP_DATANODE_BACKUP_BLOCK_POLL_TIME_KEY = "dfs.backup.datanode.backup.block.poll.time";
  public static final long DFS_BACKUP_DATANODE_BACKUP_BLOCK_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(1);

  public static final String DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_KEY = "dfs.backup.datanode.backup.handler.count";
  public static final int DFS_BACKUP_DATANODE_BACKUP_BLOCK_HANDLER_COUNT_DEFAULT = 10;

  public static final String DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_KEY = "dfs.backup.datanode.restore.handler.count";
  public static final int DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_DEFAULT = 10;
  
  public static final String DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_KEY = "dfs.backup.datanode.restore.error.pause";
  public static final long DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_DEFAULT = TimeUnit.SECONDS.toMillis(3);

  public static final String DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY = "dfs.backup.namenode.missing.blocks.poll.time";
  public static final long DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(10);

  public static final String DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY = "dfs.backup.namenode.block.check.interval";
  public static final long DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(3);

  public static final String DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY = "dfs.backup.namenode.block.check.interval.delay";
  public static final int DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT = (int) TimeUnit.HOURS.toMillis(1);

  public static final String DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY = "dfs.backup.namenode.local.dir";
  public static final String DFS_BACKUP_NAMENODE_LOCAL_DIR_DEFAULT = "file:///hdfs/sort";
}
