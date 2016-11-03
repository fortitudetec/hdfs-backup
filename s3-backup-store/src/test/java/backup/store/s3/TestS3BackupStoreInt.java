package backup.store.s3;

import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import backup.BackupConstants;
import backup.IntegrationTestBase;

public class TestS3BackupStoreInt extends IntegrationTestBase {

  private final String backupBucket = "test-hdfs-backup-bucket";
  private final String prefix = "test-hdfs-backup-" + UUID.randomUUID()
                                                          .toString();
  private boolean createdBucket;

  @Test
  public void test() {

  }

  @Override
  protected void setupBackupStore(Configuration conf) throws Exception {
    conf.setProperty(BackupConstants.DFS_BACKUP_STORE_KEY, S3BackupStore.class.getName());
    conf.setProperty(S3BackupStore.DFS_BACKUP_S3_BUCKET_NAME_KEY, backupBucket);
    conf.setProperty(S3BackupStore.DFS_BACKUP_S3_OBJECT_PREFIX_KEY, prefix);
    if (!S3BackupStore.exists(backupBucket)) {
      S3BackupStore.createBucket(backupBucket);
      createdBucket = true;
    }
  }

  @Override
  protected void teardownBackupStore() throws Exception {
    if (createdBucket) {
      S3BackupStore.removeBucket(backupBucket);
    } else {
      S3BackupStore.removeAllObjects(backupBucket, prefix);
    }
  }

}
