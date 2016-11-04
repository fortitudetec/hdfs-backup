package backup.store.local;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import backup.BackupConstants;
import backup.IntegrationTestBase;
import backup.store.local.LocalBackupStore;

public class TestLocalBackupStoreInt extends IntegrationTestBase {

  private final File tmp = new File("./target/tmp");
  private final File backup = new File(tmp, "backup");

  @Test
  public void noTest() throws Exception {

  }

  @Override
  public void setupBackupStore(Configuration conf) {
    backup.mkdirs();
    conf.setProperty(BackupConstants.DFS_BACKUP_STORE_KEY, LocalBackupStore.class.getName());
    conf.setProperty(LocalBackupStore.DFS_BACKUP_LOCALBACKUPSTORE_PATH, backup.getAbsolutePath());
  }

  @Override
  public void teardownBackupStore() throws IOException {
    FileUtils.deleteDirectory(backup);
  }

}
