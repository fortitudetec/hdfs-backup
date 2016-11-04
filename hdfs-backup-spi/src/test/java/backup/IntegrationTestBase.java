package backup;

import java.io.File;

import org.apache.commons.configuration.Configuration;

public abstract class IntegrationTestBase {

  private File parcelLocation;

  public File getParcelLocation() {
    return parcelLocation;
  }

  public void setParcelLocation(File parcelLocation) {
    this.parcelLocation = parcelLocation;
  }

  public abstract void teardownBackupStore() throws Exception;

  public abstract void setupBackupStore(Configuration conf) throws Exception;

}
