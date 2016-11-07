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
package backup.store.local;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import backup.BackupConstants;
import backup.integration.MiniClusterTestBase;

public class LocalBackupStoreMiniClusterTest extends MiniClusterTestBase {

  private final File tmp = new File("./target/tmp");
  private final File backup = new File(tmp, "backup");

  @Override
  protected void setupBackupStore(Configuration conf) {
    backup.mkdirs();
    conf.setProperty(BackupConstants.DFS_BACKUP_STORE_KEY, "backup.store.local.LocalBackupStore");
    conf.setProperty(LocalBackupStoreConstants.DFS_BACKUP_LOCALBACKUPSTORE_PATH, backup.getAbsolutePath());
  }

  @Override
  protected void teardownBackupStore() throws IOException {
    FileUtils.deleteDirectory(backup);
  }

  @Override
  protected String testArtifactId() {
    return "local-backup-store";
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
