package backup.store.s3;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import backup.BackupConstants;
import backup.store.BackupStore;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.store.LengthInputStream;

public class TestS3BackupStore {

  private static final Configuration conf = new BaseConfiguration();
  private static final String backupBucket = "test-hdfs-backup-bucket";
  private static final String prefix = "test-hdfs-backup-" + UUID.randomUUID()
                                                                 .toString();

  private static boolean createdBucket;
  private ThreadLocal<Random> random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  @BeforeClass
  public static void setup() throws Exception {
    conf.setProperty(BackupConstants.DFS_BACKUP_STORE_KEY, S3BackupStore.class.getName());
    conf.setProperty(S3BackupStoreContants.DFS_BACKUP_S3_BUCKET_NAME_KEY, backupBucket);
    conf.setProperty(S3BackupStoreContants.DFS_BACKUP_S3_OBJECT_PREFIX_KEY, prefix);
    conf.setProperty(S3BackupStoreContants.DFS_BACKUP_S3_LISTING_MAXKEYS_KEY, 9);
    if (!S3BackupStoreUtil.exists(backupBucket)) {
      S3BackupStoreUtil.createBucket(backupBucket);
      createdBucket = true;
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (createdBucket) {
      S3BackupStoreUtil.removeBucket(backupBucket);
    } else {
      S3BackupStoreUtil.removeAllObjects(backupBucket, prefix);
    }
  }

  @Test
  public void testHasBlock() throws Exception {
    BackupStore backupStore = BackupStore.create(conf);
    String poolId = "poolid";
    ExtendedBlock extendedBlock = createExtendedBlock(poolId);
    backupStore.backupBlock(extendedBlock, createInputStream(extendedBlock.getLength()),
        createInputStream(extendedBlock.getLength()));
    assertTrue(backupStore.hasBlock(extendedBlock));
  }

  @Test
  public void testGetExtendedBlocks() throws Exception {
    BackupStore backupStore = BackupStore.create(conf);
    String poolId = "poolid";
    List<ExtendedBlock> blocks = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      ExtendedBlock extendedBlock = createExtendedBlock(poolId);
      backupStore.backupBlock(extendedBlock, createInputStream(extendedBlock.getLength()),
          createInputStream(extendedBlock.getLength()));
      blocks.add(extendedBlock);
    }
    Thread.sleep(1000);
    ExtendedBlockEnum<Void> extendedBlocks = backupStore.getExtendedBlocks();
    ExtendedBlock block;
    List<ExtendedBlock> remoteBlocks = new ArrayList<>();
    while ((block = extendedBlocks.next()) != null) {
      remoteBlocks.add(block);
    }

    for (ExtendedBlock extendedBlock : blocks) {
      assertTrue(remoteBlocks.contains(extendedBlock));
    }
  }

  private ExtendedBlock createExtendedBlock(String poolId) {
    Random rand = random.get();
    long blkid = rand.nextLong();
    long len = rand.nextInt(1000);
    long genstamp = rand.nextInt(2000);
    return new ExtendedBlock(poolId, blkid, len, genstamp);
  }

  private LengthInputStream createInputStream(long len) {
    byte[] buf = new byte[(int) len];
    random.get()
          .nextBytes(buf);
    return new LengthInputStream(new ByteArrayInputStream(buf), len);
  }
}
