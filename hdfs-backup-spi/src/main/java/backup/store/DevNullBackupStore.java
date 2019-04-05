package backup.store;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;

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
  public ExtendedBlockEnum<Void> getExtendedBlocks() {
    return ExtendedBlockEnum.EMPTY;
  }

  @Override
  public void deleteBlock(ExtendedBlock extendedBlock) {

  }

  @Override
  public void destroyAllBlocks() throws Exception {

  }

  @Override
  public List<ExtendedBlock> getExtendedBlocks(ExtendedBlock extendedBlock) throws Exception {
    throw new RuntimeException();
  }

  @Override
  public void deleteBlocks(Collection<ExtendedBlock> deletes) throws Exception {
    
  }

}
