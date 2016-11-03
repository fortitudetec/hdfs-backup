package backup;

import org.apache.commons.configuration.Configuration;

public abstract class IntegrationTestBase {
  protected abstract void teardownBackupStore() throws Exception;

  protected abstract void setupBackupStore(Configuration conf) throws Exception;

}
