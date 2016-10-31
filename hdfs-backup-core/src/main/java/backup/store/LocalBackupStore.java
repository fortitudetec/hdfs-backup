package backup.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

public class LocalBackupStore extends BackupStore {

  public static final String DFS_BACKUP_LOCALBACKUPSTORE_PATH = "dfs.backup.localbackupstore.path";

  private File dir;

  @Override
  public void init() throws Exception {
    Configuration configuration = getConf();
    String localPath = configuration.get(DFS_BACKUP_LOCALBACKUPSTORE_PATH);
    dir = new File(localPath);
    dir.mkdirs();
  }

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData) throws Exception {
    File dataFile = getDataFile(extendedBlock);
    File metaDataFile = getMetaDataFile(extendedBlock);
    try (FileOutputStream output = new FileOutputStream(dataFile)) {
      IOUtils.copy(data, output);
    }
    try (FileOutputStream output = new FileOutputStream(metaDataFile)) {
      IOUtils.copy(metaData, output);
    }
  }

  private File getMetaDataFile(ExtendedBlock extendedBlock) {
    return new File(dir, Long.toString(extendedBlock.getBlockId()) + ".meta");
  }

  private File getDataFile(ExtendedBlock extendedBlock) {
    return new File(dir, Long.toString(extendedBlock.getBlockId()) + ".data");
  }

  @Override
  public boolean hasBlock(ExtendedBlock extendedBlock) throws Exception {
    File metaDataFile = getMetaDataFile(extendedBlock);
    if (metaDataFile.exists()) {
      File dataFile = getDataFile(extendedBlock);
      if (dataFile.exists()) {
        return true;
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

}
