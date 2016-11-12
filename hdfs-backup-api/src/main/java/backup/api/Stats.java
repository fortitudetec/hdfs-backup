package backup.api;

public interface Stats {

  int getFinalizedBlocksSizeCount();

  int getFutureChecksSizeCount();

  int getBackupsInProgressCount();

  int getRestoreBlocks();

  int getRestoresInProgressCount();

  double getBackupBytesPerSecond();

  double getRestoreBytesPerSecond();

}
