package backup;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import backup.store.LocalBackupStore;

public class TestLocalBackupStore extends MiniClusterTestBase {

  private final File backup = new File(tmp, "backup");

  @Test
  public void test() {

  }

  @Override
  protected void setupBackupStore(Configuration conf) {
    backup.mkdirs();
    conf.set(BackupConstants.DFS_BACKUP_STORE_KEY, LocalBackupStore.class.getName());
    conf.set(LocalBackupStore.DFS_BACKUP_LOCALBACKUPSTORE_PATH, backup.getAbsolutePath());
  }

  @Override
  protected void teardownBackupStore() {
    rmr(backup);
  }

}
