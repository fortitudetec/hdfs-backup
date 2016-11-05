package backup.datanode;

import java.io.IOException;

import backup.store.WritableExtendedBlock;

public class BackupRPCImpl implements BackupRPC {

  private DataNodeBackupProcessor dataNodeBackupProcessor;
  private DataNodeRestoreProcessor dataNodeRestoreProcessor;

  public BackupRPCImpl(DataNodeBackupProcessor dataNodeBackupProcessor,
      DataNodeRestoreProcessor dataNodeRestoreProcessor) {
    this.dataNodeBackupProcessor = dataNodeBackupProcessor;
    this.dataNodeRestoreProcessor = dataNodeRestoreProcessor;
  }

  @Override
  public void backupBlock(WritableExtendedBlock extendedBlock) throws IOException {
    dataNodeBackupProcessor.backupBlock(extendedBlock);
  }

  @Override
  public void restoreBlock(WritableExtendedBlock extendedBlock) throws IOException {
    dataNodeRestoreProcessor.addToRestoreQueue(extendedBlock);
  }

}
