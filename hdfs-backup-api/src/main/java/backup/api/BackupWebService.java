package backup.api;

import java.io.IOException;

public interface BackupWebService<T extends Stats> {
  T getStats() throws IOException;

  void runGc(boolean debug) throws IOException;

}
