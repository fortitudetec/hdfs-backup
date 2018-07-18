package backup.namenode.report;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import backup.namenode.NameNodeBackupBlockCheckProcessor.ExtendedBlockWithAddress;
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

  void deleteBackupBlock(ExtendedBlock block);

  void deleteBackupBlockError(ExtendedBlock block);

  void restoreBlockError(ExtendedBlock block);

  void backupRequestError(InetSocketAddress dataNodeAddress, ExtendedBlockWithAddress extendedBlockWithAddress);

  void statusBlockMetaDataFetchFromNameNode(String src);

  void statusExtendedBlocksFromNameNode(String src, ExtendedBlock extendedBlock, DatanodeInfo[] locations);

}