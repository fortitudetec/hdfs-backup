package backup.store;

import java.io.InputStream;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

public class DevNullBackupStore extends BackupStore {

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream meta) throws Exception {

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
  public void init() throws Exception {

  }

}
