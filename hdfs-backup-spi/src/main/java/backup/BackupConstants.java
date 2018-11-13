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

  public static final String DFS_BACKUP_REMOTE_BACKUP_BATCH_KEY = "dfs.backup.remote.backup.batch";
  public static final int DFS_BACKUP_REMOTE_BACKUP_BATCH_DEFAULT = 100;

  public static final String DFS_DATANODE_BACKUP_FSDATASET_FACTORY_KEY = "dfs.datanode.backup.fsdataset.factory";

  public static final String DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_KEY = "dfs.backup.datanode.restore.handler.count";
  public static final int DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_DEFAULT = 10;

  public static final String DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_KEY = "dfs.backup.datanode.restore.error.pause";
  public static final long DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_DEFAULT = TimeUnit.SECONDS.toMillis(3);

  public static final String DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY = "dfs.backup.namenode.block.check.interval";
  public static final long DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(3);

  public static final String DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY = "dfs.backup.namenode.block.check.interval.delay";
  public static final long DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT = TimeUnit.HOURS.toMillis(1);

  public static final String DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY = "dfs.backup.namenode.local.dir";

  public static final String DFS_BACKUP_DATANODE_RPC_PORT_KEY = "dfs.backup.datanode.rpc.port";
  public static final int DFS_BACKUP_DATANODE_RPC_PORT_DEFAULT = 50888;

  public static final String DFS_BACKUP_NAMENODE_RPC_PORT_KEY = "dfs.backup.namenode.rpc.port";
  public static final int DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT = 50889;

  public static final String DFS_BACKUP_NAMENODE_HTTP_PORT_KEY = "dfs.backup.namenode.http.port";
  public static final int DFS_BACKUP_NAMENODE_HTTP_PORT_DEFAULT = 50890;

  public static final String DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY = "dfs.backup.namenode.missing.blocks.poll.time";
  public static final long DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT = TimeUnit.SECONDS.toMillis(10);

  public static final String DFS_BACKUP_DATANODE_BACKUP_THREAD_COUNT_KEY = "dfs.backup.datanode.backup.thread.count";
  public static final int DFS_BACKUP_DATANODE_BACKUP_THREAD_COUNT_DEFAULT = 1;

  public static final String DFS_BACKUP_DATANODE_BACKUP_QUEUE_DEPTH_KEY = "dfs.backup.datanode.backup.queue.depth";
  public static final int DFS_BACKUP_DATANODE_BACKUP_QUEUE_DEPTH_DEFAULT = 10_000;

  public static final String DFS_BACKUP_DATANODE_BACKUP_AGE_KEY = "dfs.backup.datanode.backup.age";
  public static final long DFS_BACKUP_DATANODE_BACKUP_AGE_DEFAULT = TimeUnit.SECONDS.toMillis(5);

  public static final String DFS_BACKUP_DATANODE_BACKUP_RETRY_DELAY_KEY = "dfs.backup.datanode.backup.retry.delay";
  public static final long DFS_BACKUP_DATANODE_RETRY_DELAY_DEFAULT = TimeUnit.MINUTES.toSeconds(1);

  public static final String DFS_BACKUP_IGNORE_PATH_FILE_KEY = "DFS_BACKUP_IGNORE_PATH_FILE";
  public static final String DFS_BACKUP_IGNORE_PATH_FILE_DEFAULT = "/backup/ignore";

}
