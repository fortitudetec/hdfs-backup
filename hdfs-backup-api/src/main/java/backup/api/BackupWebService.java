package backup.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface BackupWebService<T extends Stats> {
  T getStats() throws IOException;

  void runReport() throws IOException;

  List<String> listReports() throws IOException;

  InputStream getReport(String id) throws IOException;
}
