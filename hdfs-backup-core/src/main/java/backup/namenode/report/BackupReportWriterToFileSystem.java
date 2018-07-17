package backup.namenode.report;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.store.ExtendedBlock;

public class BackupReportWriterToFileSystem implements BackupReportWriter {

  private static final Logger LOG = LoggerFactory.getLogger(BackupReportWriterToFileSystem.class);

  public static final String REPORT = "report.";
  public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
  private final PrintWriter _output;

  public BackupReportWriterToFileSystem(FileSystem fileSystem, Path path) throws IOException {
    fileSystem.mkdirs(path);
    SimpleDateFormat format = new SimpleDateFormat(YYYYMMDDHHMMSS);
    gcOldReports(fileSystem, path, format, TimeUnit.DAYS, 7);
    String timestamp = format.format(new Date());
    _output = new PrintWriter(fileSystem.create(new Path(path, REPORT + timestamp)));
  }

  public static List<String> pruneOldReports(SimpleDateFormat format, List<String> reports, TimeUnit timeUnit,
      long time) {
    long now = System.currentTimeMillis();
    List<String> delete = new ArrayList<>();
    for (String s : reports) {
      if (s.startsWith(REPORT)) {
        String ts = s.substring(REPORT.length());
        try {
          Date date = format.parse(ts);
          if (date.getTime() + timeUnit.toMillis(time) < now) {
            delete.add(s);
          }
        } catch (ParseException e) {
          LOG.error("Can not parse " + ts, e);
        }
      }
    }
    return delete;
  }

  private void gcOldReports(FileSystem fileSystem, Path path, SimpleDateFormat format, TimeUnit timeUnit, long time)
      throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(path);
    List<String> reports = new ArrayList<>();
    for (FileStatus fileStatus : listStatus) {
      String name = fileStatus.getPath()
                              .getName();
      reports.add(name);
    }
    List<String> pruneOldReports = pruneOldReports(format, reports, timeUnit, time);
    for (String s : pruneOldReports) {
      Path reportFile = new Path(path, s);
      LOG.info("Removing old report {}", reportFile);
      fileSystem.delete(reportFile, false);
    }
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
