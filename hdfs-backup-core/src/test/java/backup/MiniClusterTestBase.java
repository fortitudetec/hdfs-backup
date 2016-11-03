package backup;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

import backup.datanode.BackupFsDatasetSpiFactory;
import backup.datanode.DataNodeBackupServicePlugin;
import backup.namenode.NameNodeBackupProcessor;
import backup.namenode.NameNodeBackupServicePlugin;
import backup.store.BackupStore;
import backup.store.ConfigurationConverter;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;

public abstract class MiniClusterTestBase {

  protected final File tmp = new File("./target/tmp");
  protected final String zkConnection = "localhost/backup";

  @Test
  public void integrationBasicTest() throws Exception {
    File hdfsDir = setupHdfsLocalDir();
    rmrZk(zkConnection, "/");
    Configuration conf = setupConfig(hdfsDir, zkConnection);

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    Thread thread = null;
    try {
      DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
      Path path = new Path("/testing.txt");
      writeFile(fileSystem, path);
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

  private File setupHdfsLocalDir() throws IOException {
    FileUtils.deleteDirectory(tmp);
    File hdfsDir = new File(tmp, "testHdfs-" + UUID.randomUUID());
    hdfsDir.mkdirs();
    return hdfsDir;
  }

  @Test
  public void integrationBlockCheckWhenAllBackupStoreBlocksMissingTest() throws Exception {
    File hdfsDir = setupHdfsLocalDir();
    rmrZk(zkConnection, "/");
    Configuration conf = setupConfig(hdfsDir, zkConnection);

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    Thread thread = null;
    try {
      DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
      System.out.println(fileSystem.listStatus(new Path("/")));
      Path path = new Path("/testing.txt");
      writeFile(fileSystem, path);
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

      BackupStore backupStore = BackupStore.create(ConfigurationConverter.convert(conf));
      Set<ExtendedBlock> original = toSet(backupStore.getExtendedBlocks());
      destroyBackupStoreBlocks(backupStore);

      NameNode nameNode = hdfsCluster.getNameNode();
      NameNodeBackupProcessor processor = SingletonManager.getManager(NameNodeBackupProcessor.class)
                                                          .getInstance(nameNode);
      processor.runBlockCheck();

      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

      Set<ExtendedBlock> current = toSet(backupStore.getExtendedBlocks());

      assertEquals(original, current);

    } finally {
      if (thread != null) {
        thread.interrupt();
      }
      hdfsCluster.shutdown();
      teardownBackupStore();
    }
  }

  @Test
  public void integrationBlockCheckWhenSomeBackupStoreBlocksMissingTest() throws Exception {
    File hdfsDir = setupHdfsLocalDir();
    rmrZk(zkConnection, "/");
    Configuration conf = setupConfig(hdfsDir, zkConnection);

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    Thread thread = null;
    try {
      DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
      writeFile(fileSystem, new Path("/testing1.txt"));
      writeFile(fileSystem, new Path("/testing2.txt"));
      writeFile(fileSystem, new Path("/testing3.txt"));
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

      BackupStore backupStore = BackupStore.create(ConfigurationConverter.convert(conf));
      Set<ExtendedBlock> original = toSet(backupStore.getExtendedBlocks());
      destroyOneBackupStoreBlock(backupStore);

      NameNode nameNode = hdfsCluster.getNameNode();

      NameNodeBackupProcessor processor = SingletonManager.getManager(NameNodeBackupProcessor.class)
                                                          .getInstance(nameNode);
      processor.runBlockCheck();

      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

      Set<ExtendedBlock> current = toSet(backupStore.getExtendedBlocks());

      assertEquals(original, current);

    } finally {
      if (thread != null) {
        thread.interrupt();
      }
      hdfsCluster.shutdown();
      teardownBackupStore();
    }
  }

  private void writeFile(DistributedFileSystem fileSystem, Path path) throws IOException {
    try (FSDataOutputStream outputStream = fileSystem.create(path)) {
      outputStream.write("abc".getBytes());
    }
  }

  private void destroyOneBackupStoreBlock(BackupStore backupStore) throws Exception {
    Set<ExtendedBlock> list = toSet(backupStore.getExtendedBlocks());
    for (ExtendedBlock extendedBlock : list) {
      backupStore.deleteBlock(extendedBlock);
      return;
    }
  }

  private void destroyBackupStoreBlocks(BackupStore backupStore) throws Exception {
    Set<ExtendedBlock> list = toSet(backupStore.getExtendedBlocks());
    for (ExtendedBlock extendedBlock : list) {
      backupStore.deleteBlock(extendedBlock);
    }
  }

  private static Set<ExtendedBlock> toSet(ExtendedBlockEnum e) throws Exception {
    Set<ExtendedBlock> set = new HashSet<>();
    ExtendedBlock block;
    while ((block = e.next()) != null) {
      set.add(block);
    }
    return set;
  }

  private Configuration setupConfig(File hdfsDir, String zkConnection) throws Exception {
    Configuration conf = new Configuration();
    File backup = new File(tmp, "backup");
    backup.mkdirs();
    conf.set(DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY, backup.getAbsolutePath());
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
    return conf;
  }

  protected abstract void teardownBackupStore() throws Exception;

  protected abstract void setupBackupStore(Configuration conf) throws Exception;

  public static void rmrZk(String zkConnection, String path) throws Exception {
    try (ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zkConnection, 30000)) {
      ZkUtils.rmr(zooKeeper, path);
    }
  }
}
