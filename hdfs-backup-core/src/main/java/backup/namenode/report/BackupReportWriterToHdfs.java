package backup.namenode.report;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import backup.store.ExtendedBlock;

public class BackupReportWriterToHdfs implements BackReportWriter {

  private static final String YYYYMMDDHHMMSS = "YYYYMMddHHmmss";
  private final PrintWriter _output;

  public BackupReportWriterToHdfs(DistributedFileSystem fileSystem, Path path) throws IOException {
    fileSystem.mkdirs(path);
    SimpleDateFormat format = new SimpleDateFormat(YYYYMMDDHHMMSS);
    String timestamp = format.format(new Date());
    _output = new PrintWriter(fileSystem.create(new Path(path, "report." + timestamp)));
  }

  @Override
  public void start() {
    _output.println("start");
  }

  @Override
  public void complete() {
    _output.println("complete");
  }

  @Override
  public void startBlockMetaDataFetchFromNameNode() {
    _output.println("startBlockMetaDataFetchFromNameNode");
  }

  @Override
  public void completeBlockMetaDataFetchFromNameNode() {
    _output.println("completeBlockMetaDataFetchFromNameNode");
  }

  @Override
  public void startBlockPoolCheck(String blockPoolId) {
    _output.println("startBlockPoolCheck " + blockPoolId);
  }

  @Override
  public void completeBlockPoolCheck(String blockPoolId) {
    _output.println("completeBlockPoolCheck " + blockPoolId);
  }

  @Override
  public void startRestoreAll() {
    _output.println("startRestoreAll");
  }

  @Override
  public void completeRestoreAll() {
    _output.println("completeRestoreAll");
  }

  @Override
  public void restoreBlock(ExtendedBlock block) {
    _output.println("restoreBlock " + block);
  }

  @Override
  public void startBackupAll() {
    _output.println("startBackupAll");
  }

  @Override
  public void completeBackupAll() {
    _output.println("completeBackupAll");
  }

  @Override
  public void backupRequestBatch(List<?> batch) {
    _output.println("backupRequestBatch size " + batch.size());
  }

  @Override
  public void deleteBackupBlock(ExtendedBlock bu) {
    _output.println("deleteBackupBlock " + bu);
  }

  @Override
  public void close() throws IOException {
    _output.close();
  }

}
