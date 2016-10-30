package backup.store;

import java.io.InputStream;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class DevNullBackupStore extends BackupStore {

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, InputStream data, InputStream meta) throws Exception {

  }

  @Override
  public InputStream getMetaDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    throw new RuntimeException();
  }

  @Override
  public InputStream getDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    throw new RuntimeException();
  }

  @Override
  public boolean hasBlock(ExtendedBlock extendedBlock) throws Exception {
    return false;
  }

  @Override
  public void init() {
    
  }

}
