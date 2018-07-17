package backup.namenode.report;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class BackupReportWriterToFileSystemTest {

  @Test
  public void testPruneOldFiles1() {
    SimpleDateFormat format = new SimpleDateFormat(BackupReportWriterToFileSystem.YYYYMMDDHHMMSS);
    long now = System.currentTimeMillis();
    int hours = 500;
    List<String> timestamp = new ArrayList<>();
    for (int i = 0; i < hours; i++) {
      String f = format.format(new Date(now - TimeUnit.HOURS.toMillis(i)));
      timestamp.add(BackupReportWriterToFileSystem.REPORT + f);
    }
    List<String> reportsToRemove = BackupReportWriterToFileSystem.pruneOldReports(format, timestamp, TimeUnit.DAYS, 7);
    assertEquals(332, reportsToRemove.size());
  }

  @Test
  public void testPruneOldFiles2() {
    SimpleDateFormat format = new SimpleDateFormat(BackupReportWriterToFileSystem.YYYYMMDDHHMMSS);
    long now = System.currentTimeMillis();
    int hours = 5;
    List<String> timestamp = new ArrayList<>();
    for (int i = 0; i < hours; i++) {
      String f = format.format(new Date(now - TimeUnit.HOURS.toMillis(i)));
      timestamp.add(BackupReportWriterToFileSystem.REPORT + f);
    }
    List<String> reportsToRemove = BackupReportWriterToFileSystem.pruneOldReports(format, timestamp, TimeUnit.DAYS, 7);
    assertEquals(0, reportsToRemove.size());
  }
}
