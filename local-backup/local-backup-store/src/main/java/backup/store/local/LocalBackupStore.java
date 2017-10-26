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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterators;

import backup.store.BackupStore;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.store.LengthInputStream;

public class LocalBackupStore extends BackupStore {

  private final static Logger LOG = LoggerFactory.getLogger(LocalBackupStore.class);

  private static final String META = ".meta";
  private static final String DATA = ".data";

  private File dir;

  @Override
  public void init() throws Exception {
    Configuration configuration = getConf();
    String localPath = configuration.getString(LocalBackupStoreConstants.DFS_BACKUP_LOCALBACKUPSTORE_PATH);
    dir = new File(localPath);
    dir.mkdirs();
  }

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData)
      throws Exception {
    LOG.info("before sleep backupBlock {} dataLength {} metaDataLength", extendedBlock, data.getLength(), metaData.getLength());
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    LOG.info("after sleep backupBlock {} dataLength {} metaDataLength", extendedBlock, data.getLength(), metaData.getLength());
    File dataFile = getDataFile(extendedBlock);
    File metaDataFile = getMetaDataFile(extendedBlock);
    try (FileOutputStream output = new FileOutputStream(dataFile)) {
      IOUtils.copy(data, output);
    }
    if (dataFile.length() != extendedBlock.getLength()) {
      throw new IOException(
          "length of backup file " + dataFile.length() + " different than block " + extendedBlock.getLength());
    }
    try (FileOutputStream output = new FileOutputStream(metaDataFile)) {
      IOUtils.copy(metaData, output);
    }
  }

  @Override
  public boolean hasBlock(ExtendedBlock extendedBlock) throws Exception {
    File metaDataFile = getMetaDataFile(extendedBlock);
    if (metaDataFile.exists()) {
      File dataFile = getDataFile(extendedBlock);
      if (dataFile.exists()) {
        return dataFile.length() == extendedBlock.getLength();
      }
    }
    return false;
  }

  @Override
  public InputStream getMetaDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    File metaDataFile = getMetaDataFile(extendedBlock);
    return new FileInputStream(metaDataFile);
  }

  @Override
  public InputStream getDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    File dataFile = getDataFile(extendedBlock);
    return new FileInputStream(dataFile);
  }

  @Override
  public ExtendedBlockEnum<Void> getExtendedBlocks() {
    File[] pools = dir.listFiles((FileFilter) pathname -> pathname.isDirectory());
    Builder<Iterator<ExtendedBlock>> builder = ImmutableList.builder();
    for (File pool : pools) {
      builder.add(getExtendedBlocks(pool));
    }
    return ExtendedBlockEnum.toExtendedBlockEnum(Iterators.concat(builder.build()
                                                                         .iterator()));
  }

  @Override
  public void deleteBlock(ExtendedBlock extendedBlock) {
    getDataFile(extendedBlock).delete();
    getMetaDataFile(extendedBlock).delete();
  }

  private Iterator<ExtendedBlock> getExtendedBlocks(File pool) {
    List<File> list = Arrays.asList(pool.listFiles((FileFilter) pathname -> pathname.getName()
                                                                                    .endsWith(DATA)));
    Iterator<File> iterator = list.iterator();
    return new Iterator<ExtendedBlock>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public ExtendedBlock next() {
        return toExtendedBlock(iterator.next());
      }
    };
  }

  private ExtendedBlock toExtendedBlock(File block) {
    String blockPoolId = block.getParentFile()
                              .getName();
    String name = block.getName();
    name = name.substring(0, name.indexOf('.'));
    ImmutableList<String> list = ImmutableList.copyOf(Splitter.on('_')
                                                              .split(name));
    long blockId = Long.parseLong(list.get(0));
    long genstamp = Long.parseLong(list.get(1));
    return new ExtendedBlock(blockPoolId, blockId, block.length(), genstamp);
  }

  private File getMetaDataFile(ExtendedBlock extendedBlock) {
    return new File(getPool(extendedBlock), getBlockName(extendedBlock) + META);
  }

  private File getDataFile(ExtendedBlock extendedBlock) {
    return new File(getPool(extendedBlock), getBlockName(extendedBlock) + DATA);
  }

  private File getPool(ExtendedBlock extendedBlock) {
    File pool = new File(dir, extendedBlock.getPoolId());
    pool.mkdirs();
    return pool;
  }

  private String getBlockName(ExtendedBlock extendedBlock) {
    return Long.toString(extendedBlock.getBlockId()) + "_" + Long.toString(extendedBlock.getGenerationStamp());
  }

  @Override
  public void destroyAllBlocks() throws Exception {
    FileUtils.deleteDirectory(dir);
  }

}
