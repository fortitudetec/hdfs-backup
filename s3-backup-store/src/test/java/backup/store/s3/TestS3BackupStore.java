package backup.store.s3;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import backup.BackupConstants;
import backup.MiniClusterTestBase;

public class TestS3BackupStore extends MiniClusterTestBase {

  private final String backupBucket = "test-hdfs-backup-bucket";
  private final String prefix = "test-hdfs-backup-" + UUID.randomUUID()
                                                          .toString();
  private boolean createdBucket;
  
  @Test
  public void test() {
    
  }

  @Override
  protected void setupBackupStore(Configuration conf) throws Exception {
    conf.set(BackupConstants.DFS_BACKUP_STORE_KEY, S3BackupStore.class.getName());
    conf.set(S3BackupStore.DFS_BACKUP_S3_BUCKET_NAME, backupBucket);
    conf.set(S3BackupStore.DFS_BACKUP_S3_OBJECT_PREFIX, prefix);
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
