package backup.namenode;

import backup.store.ExtendedBlock;

public interface BackupStoreDeleter {

  void deleteBlock(ExtendedBlock block) throws Exception;

}
