package backup.namenode.report;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.store.ExtendedBlock;

public class LoggerBackupReportWriter implements BackReportWriter {

  private static final Logger LOG = LoggerFactory.getLogger(LoggerBackupReportWriter.class);

  @Override
  public void start() {
    LOG.info("start");
  }

  @Override
  public void complete() {
    LOG.info("complete");
  }

  @Override
  public void startBlockMetaDataFetchFromNameNode() {
    LOG.info("startBlockMetaDataFetchFromNameNode");
  }

  @Override
  public void completeBlockMetaDataFetchFromNameNode() {
    LOG.info("completeBlockMetaDataFetchFromNameNode");
  }

  @Override
  public void startBlockPoolCheck(String blockPoolId) {
    LOG.info("startBlockPoolCheck " + blockPoolId);
  }

  @Override
  public void completeBlockPoolCheck(String blockPoolId) {
    LOG.info("completeBlockPoolCheck " + blockPoolId);
  }

  @Override
  public void startRestoreAll() {
    LOG.info("startRestoreAll");
  }

  @Override
  public void completeRestoreAll() {
    LOG.info("completeRestoreAll");
  }

  @Override
  public void restoreBlock(ExtendedBlock block) {
    LOG.info("restoreBlock " + block);
  }

  @Override
  public void startBackupAll() {
    LOG.info("startBackupAll");
  }

  @Override
  public void completeBackupAll() {
    LOG.info("completeBackupAll");
  }

  @Override
  public void backupRequestBatch(List<?> batch) {
    LOG.info("backupRequestBatch size " + batch.size());
  }

  @Override
  public void deleteBackupBlock(ExtendedBlock bu) {
    LOG.info("deleteBackupBlock " + bu);
  }

  @Override
  public void close() throws IOException {

  }

}
