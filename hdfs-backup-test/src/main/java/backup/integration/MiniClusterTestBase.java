/*
 * Copyright 2016 Fortitude Technologies LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backup.integration;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
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
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.BackupConstants;
import backup.SingletonManager;
import backup.datanode.BackupFsDatasetSpiFactory;
import backup.datanode.DataNodeBackupServicePlugin;
import backup.namenode.NameNodeBackupServicePlugin;
import backup.namenode.NameNodeRestoreProcessor;
import backup.store.BackupStore;
import backup.store.BackupStoreClassHelper;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;

public abstract class MiniClusterTestBase {

  private final static Logger LOG = LoggerFactory.getLogger(MiniClusterTestBase.class);

  protected static final String USER_HOME = "user.home";
  protected static final String LIB = "lib";
  protected static final String TESTS = "tests";
  protected static final String TAR_GZ = "tar.gz";
  protected static final String JAR = "jar";
  protected final File tmpHdfs = new File("./target/tmp_hdfs");
  protected final File tmpParcel = new File("./target/tmp_parcel");
  protected final String zkConnection = "localhost/backup";
  protected boolean classLoaderLoaded;

  protected abstract void setupBackupStore(org.apache.commons.configuration.Configuration conf) throws Exception;

  protected abstract String testArtifactId();

  protected abstract String testGroupName();

  protected abstract String testVersion();

  @Before
  public void setup() throws Exception {
    setupClassLoader();
  }

  protected void setupClassLoader() throws Exception {
    if (!classLoaderLoaded) {
      String manveRepo = System.getProperty(USER_HOME) + "/.m2/repository";
      LocalRepository localRepo = new LocalRepository(manveRepo);
      File extractTar = extractTar(tmpParcel, localRepo);
      File libDir = findLibDir(extractTar);
      BackupStoreClassHelper.loadClassLoaders(libDir.getAbsolutePath());
      classLoaderLoaded = true;
    }
  }

  @Test
  public void testIntegrationBasic() throws Exception {
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
      AtomicBoolean success = new AtomicBoolean(false);
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
                  LOG.info("Missing block restored.");
                  success.set(true);
                  return;
                }
              }
            } catch (IOException e) {
              LOG.error(e.getMessage());
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
      if (!success.get()) {
        fail();
      }
    } finally {
      if (thread != null) {
        thread.interrupt();
      }
      hdfsCluster.shutdown();
      destroyBackupStore(conf);
    }
  }

  @Test
  public void testIntegrationBasicFullRestoreFromShutdown() throws Exception {
    File hdfsDir = setupHdfsLocalDir();
    rmrZk(zkConnection, "/");
    Configuration conf = setupConfig(hdfsDir, zkConnection);
    {
      MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
      try {
        DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
        for (int i = 0; i < 100; i++) {
          writeFile(fileSystem, new Path("/testing." + i + ".txt"));
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        hdfsCluster.stopDataNode(0);
        hdfsCluster.shutdownNameNodes();

        // Remove data
        FileUtils.deleteDirectory(new File(hdfsDir, "data"));

        hdfsCluster.restartNameNodes();
        hdfsCluster.startDataNodes(conf, 1, true, null, null);

        NameNode nameNode = hdfsCluster.getNameNode();
        for (int i = 0; i < 90; i++) {
          if (!nameNode.isInSafeMode()) {
            return;
          }
          System.out.println(nameNode.getState() + " " + nameNode.isInSafeMode());
          Thread.sleep(1000);
        }
        fail();
      } finally {
        hdfsCluster.shutdown();
        destroyBackupStore(conf);
      }
    }
  }

  private File setupHdfsLocalDir() throws IOException {
    FileUtils.deleteDirectory(tmpHdfs);
    File hdfsDir = new File(tmpHdfs, "testHdfs-" + UUID.randomUUID());
    hdfsDir.mkdirs();
    return hdfsDir;
  }

  @Test
  public void testIntegrationBlockCheckWhenAllBackupStoreBlocksMissing() throws Exception {
    File hdfsDir = setupHdfsLocalDir();
    rmrZk(zkConnection, "/");
    Configuration conf = setupConfig(hdfsDir, zkConnection);

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    Thread thread = null;
    try (BackupStore backupStore = BackupStore.create(BackupUtil.convert(conf))) {
      DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
      Path path = new Path("/testing.txt");
      writeFile(fileSystem, path);
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

      Set<ExtendedBlock> original = toSet(backupStore.getExtendedBlocks());
      destroyBackupStoreBlocks(backupStore);

      NameNode nameNode = hdfsCluster.getNameNode();
      NameNodeRestoreProcessor processor = SingletonManager.getManager(NameNodeRestoreProcessor.class)
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
      destroyBackupStore(conf);
    }
  }

  private void destroyBackupStore(Configuration conf) throws Exception {
    try (BackupStore backupStore = BackupStore.create(BackupUtil.convert(conf))) {
      backupStore.destroyAllBlocks();
    }
  }

  @Test
  public void testIntegrationBlockCheckWhenSomeBackupStoreBlocksMissing() throws Exception {
    File hdfsDir = setupHdfsLocalDir();
    rmrZk(zkConnection, "/");
    Configuration conf = setupConfig(hdfsDir, zkConnection);

    MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    Thread thread = null;
    try (BackupStore backupStore = BackupStore.create(BackupUtil.convert(conf))) {
      DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
      writeFile(fileSystem, new Path("/testing1.txt"));
      writeFile(fileSystem, new Path("/testing2.txt"));
      writeFile(fileSystem, new Path("/testing3.txt"));
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

      Set<ExtendedBlock> original = toSet(backupStore.getExtendedBlocks());
      destroyOneBackupStoreBlock(backupStore);

      NameNode nameNode = hdfsCluster.getNameNode();

      NameNodeRestoreProcessor processor = SingletonManager.getManager(NameNodeRestoreProcessor.class)
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
      destroyBackupStore(conf);
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

  private static Set<ExtendedBlock> toSet(ExtendedBlockEnum<?> e) throws Exception {
    Set<ExtendedBlock> set = new HashSet<>();
    ExtendedBlock block;
    while ((block = e.next()) != null) {
      set.add(block);
    }
    return set;
  }

  private Configuration setupConfig(File hdfsDir, String zkConnection) throws Exception {
    Configuration conf = new Configuration();
    File backup = new File(tmpHdfs, "backup");
    backup.mkdirs();
    conf.set(DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY, backup.getAbsolutePath());
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY, BackupFsDatasetSpiFactory.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY, DataNodeBackupServicePlugin.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY, NameNodeBackupServicePlugin.class.getName());

    conf.setInt(BackupConstants.DFS_BACKUP_DATANODE_RPC_PORT_KEY, 0);
    conf.setInt(BackupConstants.DFS_BACKUP_NAMENODE_RPC_PORT_KEY, 0);
    conf.setInt(BackupConstants.DFS_BACKUP_NAMENODE_HTTP_PORT_KEY, 0);
    conf.set(BackupConstants.DFS_BACKUP_ZOOKEEPER_CONNECTION_KEY, zkConnection);

    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 2);// 3
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY, 2);// 3
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 6000);// 30000
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 6000);// 5*60*1000

    org.apache.commons.configuration.Configuration configuration = BackupUtil.convert(conf);
    setupBackupStore(configuration);
    @SuppressWarnings("unchecked")
    Iterator<String> keys = configuration.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      conf.set(key, configuration.getString(key));
    }

    return conf;
  }

  public static void rmrZk(String zkConnection, String path) throws Exception {
    try (ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zkConnection, 30000)) {
      ZkUtils.rmr(zooKeeper, path);
    }
  }

  public File extractTar(File output, LocalRepository localRepo) throws Exception {
    DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
    locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
    locator.addService(TransporterFactory.class, FileTransporterFactory.class);
    locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
    RepositorySystem system = locator.getService(RepositorySystem.class);
    DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

    Artifact tarArtifact = getArtifact(system, session,
        new DefaultArtifact(testGroupName(), testArtifactId(), TAR_GZ, testVersion()));
    File parcelLocation = tarArtifact.getFile();
    extract(output, parcelLocation);
    return output;
  }

  private static Artifact getArtifact(RepositorySystem system, DefaultRepositorySystemSession session,
      DefaultArtifact rartifact) throws ArtifactResolutionException {
    ArtifactRequest artifactRequest = new ArtifactRequest();
    artifactRequest.setArtifact(rartifact);
    ArtifactResult artifactResult = system.resolveArtifact(session, artifactRequest);
    Artifact artifact = artifactResult.getArtifact();
    return artifact;
  }

  private static File extract(File output, File parcelLocation) throws Exception {
    try (TarArchiveInputStream tarInput = new TarArchiveInputStream(
        new GzipCompressorInputStream(new FileInputStream(parcelLocation)))) {
      ArchiveEntry entry;
      while ((entry = (ArchiveEntry) tarInput.getNextEntry()) != null) {
        LOG.info("Extracting: {}", entry.getName());
        File f = new File(output, entry.getName());
        if (entry.isDirectory()) {
          f.mkdirs();
        } else {
          f.getParentFile()
           .mkdirs();
          try (OutputStream out = new BufferedOutputStream(new FileOutputStream(f))) {
            IOUtils.copy(tarInput, out);
          }
        }
      }
    }
    return output;
  }

  private static File findLibDir(File file) {
    if (file.isDirectory()) {
      if (file.getName()
              .equals(LIB)) {
        return file;
      }
      for (File f : file.listFiles()) {
        File libDir = findLibDir(f);
        if (libDir != null) {
          return libDir;
        }
      }
    }
    return null;
  }

}
