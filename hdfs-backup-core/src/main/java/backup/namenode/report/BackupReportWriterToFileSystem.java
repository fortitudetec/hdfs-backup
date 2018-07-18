package backup.namenode.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.namenode.NameNodeBackupBlockCheckProcessor.ExtendedBlockWithAddress;
import backup.store.ExtendedBlock;

public class BackupReportWriterToFileSystem implements BackupReportWriter {

  private static final Logger LOG = LoggerFactory.getLogger(BackupReportWriterToFileSystem.class);

  public static final String REPORT = "report.";
  public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
  private final PrintWriter _output;
  private final ThreadLocal<SimpleDateFormat> _format = new ThreadLocal<SimpleDateFormat>() {

    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(YYYYMMDDHHMMSS);
    }

  };

  public BackupReportWriterToFileSystem(File dir) throws IOException {

    gcOldReports(dir, _format.get(), TimeUnit.DAYS, 7);
    String timestamp = _format.get()
                              .format(new Date());

    _output = new PrintWriter(new FileOutputStream(new File(dir, REPORT + timestamp)), true);
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

  private void gcOldReports(File dir, SimpleDateFormat format, TimeUnit timeUnit, long time) throws IOException {
    File[] listFiles = dir.listFiles();
    List<String> reports = new ArrayList<>();
    for (File file : listFiles) {
      String name = file.getName();
      reports.add(name);
    }
    List<String> pruneOldReports = pruneOldReports(format, reports, timeUnit, time);
    for (String s : pruneOldReports) {
      File file = new File(dir, s);
      LOG.info("Removing old report {}", file);
      file.delete();
    }
  }

  @Override
  public void start() {
    _output.println(ts() + "start");
  }

  @Override
  public void complete() {
    _output.println(ts() + "complete");
  }

  @Override
  public void startBlockMetaDataFetchFromNameNode() {
    _output.println(ts() + "startBlockMetaDataFetchFromNameNode");
  }

  @Override
  public void completeBlockMetaDataFetchFromNameNode() {
    _output.println(ts() + "completeBlockMetaDataFetchFromNameNode");
  }

  @Override
  public void startBlockPoolCheck(String blockPoolId) {
    _output.println(ts() + "startBlockPoolCheck " + blockPoolId);
  }

  @Override
  public void completeBlockPoolCheck(String blockPoolId) {
    _output.println(ts() + "completeBlockPoolCheck " + blockPoolId);
  }

  @Override
  public void startRestoreAll() {
    _output.println(ts() + "startRestoreAll");
  }

  @Override
  public void completeRestoreAll() {
    _output.println(ts() + "completeRestoreAll");
  }

  @Override
  public void restoreBlock(ExtendedBlock block) {
    _output.println(ts() + "restoreBlock " + block);
  }

  @Override
  public void startBackupAll() {
    _output.println(ts() + "startBackupAll");
  }

  @Override
  public void completeBackupAll() {
    _output.println(ts() + "completeBackupAll");
  }

  @Override
  public void backupRequestBatch(List<?> batch) {
    StringBuilder builder = new StringBuilder();
    for (Object o : batch) {
      builder.append(" ")
             .append(o.toString());
    }
    _output.println(ts() + "backupRequestBatch " + builder.toString());
    // _output.println(ts() + "backupRequestBatch " + batch.size());
  }

  @Override
  public void deleteBackupBlock(ExtendedBlock block) {
    _output.println(ts() + "deleteBackupBlock " + block);
  }

  @Override
  public void deleteBackupBlockError(ExtendedBlock block) {
    _output.println(ts() + "deleteBackupBlockError " + block);
  }

  @Override
  public void restoreBlockError(ExtendedBlock block) {
    _output.println(ts() + "restoreBlockError " + block);
  }

  @Override
  public void backupRequestError(InetSocketAddress dataNodeAddress, ExtendedBlockWithAddress extendedBlockWithAddress) {
    _output.println(ts() + "backupRequestError " + dataNodeAddress + " " + extendedBlockWithAddress);
  }

  @Override
  public void statusBlockMetaDataFetchFromNameNode(String src) {
    _output.println(ts() + "statusBlockMetaDataFetchFromNameNode " + src);
  }

  @Override
  public void statusExtendedBlocksFromNameNode(String src, ExtendedBlock extendedBlock, DatanodeInfo[] locations) {
    StringBuilder builder = new StringBuilder();
    for (DatanodeInfo datanodeInfo : locations) {
      builder.append(" ")
             .append(datanodeInfo.getIpAddr())
             .append(":")
             .append(datanodeInfo.getIpcPort());
    }
    _output.println(ts() + "statusExtendedBlocksFromNameNode " + src + " " + extendedBlock + " " + builder);
  }

  private String ts() {
    String ts = _format.get()
                       .format(new Date());
    return ts + " ";
  }

  @Override
  public void close() throws IOException {
    _output.close();
  }

}
