package backup.store;

import java.io.InputStream;

public class DevNullBackupStore extends BackupStore {

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream meta)
      throws Exception {

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

  @Override
  public ExtendedBlockEnum getExtendedBlocks() {
    return ExtendedBlockEnum.EMPTY;
  }

  @Override
  public void deleteBlock(ExtendedBlock extendedBlock) {

  }

}
