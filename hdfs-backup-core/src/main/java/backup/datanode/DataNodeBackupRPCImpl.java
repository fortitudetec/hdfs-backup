package backup.datanode;

import java.io.IOException;

import backup.store.WritableExtendedBlock;

public class DataNodeBackupRPCImpl implements DataNodeBackupRPC {

  private final DataNodeBackupProcessor backupProcessor;
  private final DataNodeRestoreProcessor restoreProcessor;

  public DataNodeBackupRPCImpl(DataNodeBackupProcessor backupProcessor, DataNodeRestoreProcessor restoreProcessor) {
    this.backupProcessor = backupProcessor;
    this.restoreProcessor = restoreProcessor;
  }

  @Override
  public void backupBlock(WritableExtendedBlock extendedBlock) throws IOException {
    backupProcessor.addToBackupQueue(extendedBlock);
  }

  @Override
  public void restoreBlock(WritableExtendedBlock extendedBlock) throws IOException {
    restoreProcessor.addToRestoreQueue(extendedBlock);
  }

}
