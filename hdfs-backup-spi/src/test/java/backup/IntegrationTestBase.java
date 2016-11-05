package backup;

import org.apache.commons.configuration.Configuration;

public abstract class IntegrationTestBase {

  public abstract void teardownBackupStore() throws Exception;

  public abstract void setupBackupStore(Configuration conf) throws Exception;

}
