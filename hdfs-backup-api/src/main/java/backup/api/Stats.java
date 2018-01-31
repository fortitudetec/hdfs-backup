package backup.api;

public interface Stats {

  int getBackupsInProgressCount();

  int getRestoreBlocks();

  int getRestoresInProgressCount();

  double getBackupBytesPerSecond();

  double getRestoreBytesPerSecond();

}
