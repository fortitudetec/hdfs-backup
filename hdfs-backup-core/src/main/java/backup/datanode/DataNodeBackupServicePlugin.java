package backup.datanode;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.ServicePlugin;

public class DataNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private DataNodeBackupProcessor backupProcessor;
  private DataNodeRestoreProcessor restoreProcessor;

  @Override
  public void start(Object service) {
    DataNode datanode = (DataNode) service;
    // This object is created here so that it's lifecycle follows the datanode
    try {
      backupProcessor = DataNodeBackupProcessor.newInstance(getConf(), datanode);
      restoreProcessor = DataNodeRestoreProcessor.newInstance(getConf(), datanode);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    IOUtils.closeQuietly(backupProcessor);
    IOUtils.closeQuietly(restoreProcessor);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

}
