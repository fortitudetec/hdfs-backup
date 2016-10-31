package backup;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import backup.datanode.BackupFsDatasetSpiFactory;
import backup.datanode.DataNodeBackupServicePlugin;
import backup.namenode.NameNodeBackupServicePlugin;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;

public abstract class MiniClusterTestBase {

  protected final File tmp = new File("./target/tmp");

  @Test
  public void integrationTest() throws Exception {
    rmr(tmp);
    File hdfsDir = new File(tmp, "testHdfs");
    hdfsDir.mkdirs();

    String zkConnection = "localhost/backup";
    rmrZk(zkConnection, "/");

    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY, BackupFsDatasetSpiFactory.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY, DataNodeBackupServicePlugin.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY, NameNodeBackupServicePlugin.class.getName());

    conf.set(BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION, zkConnection);

    setupBackupStore(conf);

    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 2);// 3
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY, 2);// 3
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 6000);// 30000
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 6000);// 5*60*1000

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    Thread thread = null;
    try {
      DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
      System.out.println(fileSystem.listStatus(new Path("/")));
      Path path = new Path("/testing.txt");
      try (FSDataOutputStream outputStream = fileSystem.create(path)) {
        outputStream.write("abc".getBytes());
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      thread = new Thread(new Runnable() {
        @Override
        public void run() {
          boolean beginTest = true;
          while (true) {
            try {
              try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                try (FSDataInputStream inputStream = fileSystem.open(path)) {
                  IOUtils.copy(inputStream, output);
                }
                if (beginTest) {
                  hdfsCluster.startDataNodes(conf, 1, true, null, null);
                  hdfsCluster.stopDataNode(0);
                  beginTest = false;
                } else {
                  System.out.println("YAY it restored the missing block!!!! " + output.toByteArray().length);
                  return;
                }
              }
            } catch (IOException e) {
              for (int i = 0; i < 10; i++) {
                System.err.println("Can not read file=======");
              }
            }
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      });
      thread.start();
      thread.join(TimeUnit.MINUTES.toMillis(2));
    } finally {
      if (thread != null) {
        thread.interrupt();
      }
      hdfsCluster.shutdown();
      teardownBackupStore();
    }
  }

  protected abstract void teardownBackupStore() throws Exception;

  protected abstract void setupBackupStore(Configuration conf) throws Exception;

  public static void rmrZk(String zkConnection, String path) throws Exception {
    try (ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zkConnection, 30000)) {
      ZkUtils.rmr(zooKeeper, path);
    }
  }

  public static void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

}
