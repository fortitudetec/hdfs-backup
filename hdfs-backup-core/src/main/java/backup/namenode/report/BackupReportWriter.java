package backup.namenode.report;

import java.io.Closeable;
import java.util.List;

import backup.store.ExtendedBlock;

public interface BackupReportWriter extends Closeable {

  void start();

  void complete();

  void startBlockMetaDataFetchFromNameNode();

  void completeBlockMetaDataFetchFromNameNode();

  void startBlockPoolCheck(String blockPoolId);

  void completeBlockPoolCheck(String blockPoolId);

  void startRestoreAll();

  void completeRestoreAll();

  void restoreBlock(ExtendedBlock block);

  void startBackupAll();

  void completeBackupAll();

  void backupRequestBatch(List<?> batch);

  void deleteBackupBlock(ExtendedBlock bu);

}