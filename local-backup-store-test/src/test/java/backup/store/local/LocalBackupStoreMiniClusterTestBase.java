package backup.store.local;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import backup.BackupConstants;
import backup.integration.MiniClusterTestBase;

public class LocalBackupStoreMiniClusterTestBase extends MiniClusterTestBase {

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
