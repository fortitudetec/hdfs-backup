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

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
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

  @After
  public void teardownTest() throws Exception {
    S3BackupStoreUtil.removeAllObjects(backupBucket, prefix);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (createdBucket) {
      S3BackupStoreUtil.removeBucket(backupBucket);
    }
  }

  @Test
  public void testHasBlock() throws Exception {
    try (BackupStore backupStore = BackupStore.create(conf)) {
      String poolId = "poolid";
      ExtendedBlock extendedBlock = createExtendedBlock(poolId);
      backupStore.backupBlock(extendedBlock, createInputStream(extendedBlock.getLength()),
          createInputStream(extendedBlock.getLength()));
      assertTrue(backupStore.hasBlock(extendedBlock));
    }
  }

  @Test
  public void testGetExtendedBlocks() throws Exception {
    try (BackupStore backupStore = BackupStore.create(conf)) {
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
  }

  @Test
  public void testGetExtendedBlocksFromStart() throws Exception {
    try (BackupStore backupStore = BackupStore.create(conf)) {
      String poolId = "poolid";
      List<ExtendedBlock> blocks = new ArrayList<>();
      int total = 8;
      for (int i = 0; i < total; i++) {
        ExtendedBlock extendedBlock = createExtendedBlock(poolId);
        backupStore.backupBlock(extendedBlock, createInputStream(extendedBlock.getLength()),
            createInputStream(extendedBlock.getLength()));
        blocks.add(extendedBlock);
      }
      List<ExtendedBlock> remoteBlocks;
      do {
        Thread.sleep(3000);
        remoteBlocks = backupStore.getExtendedBlocks(null);
      } while (remoteBlocks.size() < total);
      for (ExtendedBlock extendedBlock : blocks) {
        assertTrue(remoteBlocks.contains(extendedBlock));
      }
    }
  }

  @Test
  public void testGetExtendedBlocksIterate() throws Exception {
    try (BackupStore backupStore = BackupStore.create(conf)) {
      String poolId = "poolid";
      List<ExtendedBlock> blocks = new ArrayList<>();
      int total = 100;
      for (int i = 0; i < total; i++) {
        ExtendedBlock extendedBlock = createExtendedBlock(poolId);
        backupStore.backupBlock(extendedBlock, createInputStream(extendedBlock.getLength()),
            createInputStream(extendedBlock.getLength()));
        blocks.add(extendedBlock);
      }
      List<ExtendedBlock> remoteBlocks = new ArrayList<>();
      do {
        Thread.sleep(1000);
        ExtendedBlock lastKey = getLastKey(remoteBlocks);
        List<ExtendedBlock> extendedBlocks = backupStore.getExtendedBlocks(lastKey);
        remoteBlocks.addAll(extendedBlocks);
      } while (remoteBlocks.size() < total);

      for (ExtendedBlock extendedBlock : blocks) {
        assertTrue(remoteBlocks.contains(extendedBlock));
      }
    }
  }

  private ExtendedBlock getLastKey(List<ExtendedBlock> remoteBlocks) {
    Collections.sort(remoteBlocks);
    if (remoteBlocks.size() == 0) {
      return null;
    }
    return remoteBlocks.get(remoteBlocks.size() - 1);
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
