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
package backup.store.s3;

import java.util.UUID;

import org.apache.commons.configuration.Configuration;

import backup.BackupConstants;
import backup.integration.MiniClusterTestBase;

public class S3BackupStoreMiniClusterTestBase extends MiniClusterTestBase {

  private final String backupBucket = "test-hdfs-backup-bucket";
  private final String prefix = "test-hdfs-backup-" + UUID.randomUUID().toString();
  private boolean createdBucket;

  @Override
  protected void setupBackupStore(Configuration conf) throws Exception {
    conf.setProperty(BackupConstants.DFS_BACKUP_STORE_KEY, "backup.store.s3.S3BackupStore");
    conf.setProperty(S3BackupStoreContants.DFS_BACKUP_S3_BUCKET_NAME_KEY, backupBucket);
    conf.setProperty(S3BackupStoreContants.DFS_BACKUP_S3_OBJECT_PREFIX_KEY, prefix);
    if (!S3BackupStoreUtil.exists(backupBucket)) {
      S3BackupStoreUtil.createBucket(backupBucket);
      createdBucket = true;
    }
  }

  @Override
  protected void teardownBackupStore() throws Exception {
    if (createdBucket) {
      S3BackupStoreUtil.removeBucket(backupBucket);
    } else {
      S3BackupStoreUtil.removeAllObjects(backupBucket, prefix);
    }
  }

  @Override
  protected String testArtifactId() {
    return "s3-backup-store";
  }

  @Override
  protected String testGroupName() {
    return "hdfs-backup";
  }

  @Override
  protected String testVersion() {
    return "1.0";
  }

}
