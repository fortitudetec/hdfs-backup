package backup.namenode;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ServicePlugin;

public class NameNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private NameNodeBackupProcessor backupProcessor;

  @Override
  public void start(Object service) {
    NameNode namenode = (NameNode) service;
    // This object is created here so that it's lifecycle follows the namenode
    try {
      backupProcessor = NameNodeBackupProcessor.newInstance(getConf(), namenode);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    IOUtils.closeQuietly(backupProcessor);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

}
