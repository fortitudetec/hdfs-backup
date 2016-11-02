package backup.store;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterators;

public class LocalBackupStore extends BackupStore {

  public static final String DFS_BACKUP_LOCALBACKUPSTORE_PATH = "dfs.backup.localbackupstore.path";

  private static final String META = ".meta";
  private static final String DATA = ".data";

  private File dir;

  @Override
  public void init() throws Exception {
    Configuration configuration = getConf();
    String localPath = configuration.get(DFS_BACKUP_LOCALBACKUPSTORE_PATH);
    dir = new File(localPath);
    dir.mkdirs();
  }

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData)
      throws Exception {
    File dataFile = getDataFile(extendedBlock);
    File metaDataFile = getMetaDataFile(extendedBlock);
    try (FileOutputStream output = new FileOutputStream(dataFile)) {
      IOUtils.copy(data, output);
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
        return dataFile.length() == extendedBlock.getNumBytes();
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
  public ExtendedBlockEnum getExtendedBlocks() {
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
    File pool = new File(dir, extendedBlock.getBlockPoolId());
    pool.mkdirs();
    return pool;
  }

  private String getBlockName(ExtendedBlock extendedBlock) {
    return Long.toString(extendedBlock.getBlockId()) + "_" + Long.toString(extendedBlock.getGenerationStamp());
  }

}
