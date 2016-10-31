package backup;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import backup.datanode.BackupFsDatasetSpiFactory;
import backup.datanode.DatanodeBackupServicePlugin;
import backup.namenode.NameNodeBackupServicePlugin;
import backup.store.LocalBackupStore;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;

public class TestMiniCluster {

  public static void main(String[] args) throws Exception {
    File tmp = new File("./target/tmp");
    rmr(tmp);
    File hdfsDir = new File(tmp, "test_hdfs");
    hdfsDir.mkdirs();

    File backup = new File(tmp, "backup");
    backup.mkdirs();

    String zkConnection = "localhost/backup";
    rmrZk(zkConnection, "/");

    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY, BackupFsDatasetSpiFactory.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY, DatanodeBackupServicePlugin.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY, NameNodeBackupServicePlugin.class.getName());

    conf.set(BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION, zkConnection);
    conf.set(BackupConstants.DFS_BACKUP_STORE_KEY, LocalBackupStore.class.getName());
    conf.set(LocalBackupStore.DFS_BACKUP_LOCALBACKUPSTORE_PATH, backup.getAbsolutePath());

    // Setup the minicluster to react faster than the normal defaults.
    // dfs.heartbeat.interval - default: 3 seconds
    // dfs.namenode.stale.datanode.interval - default: 30 seconds
    // dfs.namenode.heartbeat.recheck-interval - default: 5 minutes
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 10000);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 10000);

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
    System.out.println(fileSystem.listStatus(new Path("/")));
    Path path = new Path("/testing.txt");
    try (FSDataOutputStream outputStream = fileSystem.create(path)) {
      outputStream.write("abc".getBytes());
    }
    Thread thread = new Thread(new Runnable() {
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
        }
      }
    });
    thread.start();
    thread.join();
    hdfsCluster.shutdown();
  }

  private static void rmrZk(String zkConnection, String path) throws Exception {
    try (ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zkConnection, 30000)) {
      ZkUtils.rmr(zooKeeper, path);
    }
  }

  private static void rmr(File file) {
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
