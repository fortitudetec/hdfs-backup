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
package backup.namenode;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_HTTP_PORT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_HTTP_PORT_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_RPC_PORT_KEY;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.util.ServicePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import backup.SingletonManager;
import backup.api.StatsService;
import backup.datanode.ipc.DataNodeBackupRPC;
import backup.namenode.ipc.NameNodeBackupRPC;
import backup.namenode.ipc.NameNodeBackupRPCImpl;
import classloader.FileClassLoader;
import ducktyping.DuckTypeUtil;

public class NameNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupServicePlugin.class);

  private static final String HDFS_BACKUP_STATUS = "hdfs-backup-status";
  private static final String TMP = "tmp-";
  private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
  private static final String HDFS_BACKUP_STATUS_RESOURCES_ZIP = "/hdfs-backup-status-resources.zip";
  private static final String BACKUP_STATUS_BACKUP_STATUS_SERVER = "backup.status.BackupStatusServer";
  private static final String HDFS_BACKUP_STATUS_DIR_PROP = "hdfs.backup.status.dir";
  private static final String HDFS_BACKUP_STATUS_DIR_ENV = "HDFS_BACKUP_STATUS_DIR";

  private NameNodeRestoreProcessor restoreProcessor;
  private Server server;
  private HttpServer httpServer;

  @Override
  public void start(Object service) {
    NameNode namenode = (NameNode) service;
    RPC.setProtocolEngine(getConf(), DataNodeBackupRPC.class, WritableRpcEngine.class);
    RPC.setProtocolEngine(getConf(), NameNodeBackupRPC.class, WritableRpcEngine.class);
    // This object is created here so that it's lifecycle follows the namenode
    try {
      restoreProcessor = SingletonManager.getManager(NameNodeRestoreProcessor.class).getInstance(namenode,
          () -> new NameNodeRestoreProcessor(getConf(), namenode));

      InetSocketAddress listenerAddress = namenode.getServiceRpcAddress();
      int ipcPort = listenerAddress.getPort();
      String bindAddress = listenerAddress.getAddress().getHostAddress();
      int port = getConf().getInt(DFS_BACKUP_NAMENODE_RPC_PORT_KEY, DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT);
      if (port == 0) {
        port = ipcPort + 1;
      }
      NameNodeBackupRPC nodeBackupRPCImpl = new NameNodeBackupRPCImpl(getConf(), namenode, restoreProcessor);

      server = new RPC.Builder(getConf()).setBindAddress(bindAddress).setPort(port).setInstance(nodeBackupRPCImpl)
          .setProtocol(NameNodeBackupRPC.class).build();
      server.start();
      LOG.info("NameNode Backup RPC listening on {}", port);

      int httpPort = getConf().getInt(DFS_BACKUP_NAMENODE_HTTP_PORT_KEY, DFS_BACKUP_NAMENODE_HTTP_PORT_DEFAULT);
      if (httpPort != 0) {
        ClassLoader classLoader = getClassLoader();
        if (classLoader != null) {
          ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
          try {
            // Have to setup classloader in thread context to get the static
            // files in the http server tp be setup correctly.
            Thread.currentThread().setContextClassLoader(classLoader);
            Class<?> backupStatusServerClass = classLoader.loadClass(BACKUP_STATUS_BACKUP_STATUS_SERVER);
            Object server = DuckTypeUtil.newInstance(backupStatusServerClass,
                new Class[] { Integer.TYPE, StatsService.class }, new Object[] { httpPort, nodeBackupRPCImpl });
            httpServer = DuckTypeUtil.wrap(HttpServer.class, server);
            httpServer.start();
            LOG.info("NameNode Backup HTTP listening on {}", httpPort);
          } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
          }
        } else {
          LOG.info("NameNode Backup HTTP classes not found.");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static interface HttpServer {

    void start();

    void stop();
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop();
    }
    if (httpServer != null) {
      httpServer.stop();
    }
    IOUtils.closeQuietly(restoreProcessor);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  private ClassLoader getClassLoader() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    LOG.info("Looking for {} in classpath", HDFS_BACKUP_STATUS_RESOURCES_ZIP);
    InputStream inputStream = classLoader.getResourceAsStream(HDFS_BACKUP_STATUS_RESOURCES_ZIP);
    if (inputStream == null) {
      LOG.info("Checking jvm property {}", HDFS_BACKUP_STATUS_DIR_PROP);
      String filePath = System.getProperty(HDFS_BACKUP_STATUS_DIR_PROP);
      if (filePath != null) {
        inputStream = new FileInputStream(filePath);
      }
      if (inputStream == null) {
        LOG.info("Checking env property {}", HDFS_BACKUP_STATUS_DIR_ENV);
        filePath = System.getProperty(HDFS_BACKUP_STATUS_DIR_ENV);
        if (filePath != null) {
          inputStream = new FileInputStream(filePath);
        }
      }
    }

    if (inputStream == null) {
      LOG.info("{} not found", HDFS_BACKUP_STATUS_RESOURCES_ZIP);
      return null;
    } else {
      try {
        return getClassLoader(inputStream);
      } finally {
        inputStream.close();
      }
    }
  }

  private static ClassLoader getClassLoader(InputStream zipFileInput) throws IOException, FileNotFoundException {
    File tmpDir = new File(System.getProperty(JAVA_IO_TMPDIR), HDFS_BACKUP_STATUS);
    File dir = new File(tmpDir, TMP + System.nanoTime());
    Closer closer = Closer.create();
    closer.register((Closeable) () -> FileUtils.deleteDirectory(dir));
    Runtime.getRuntime().addShutdownHook(new Thread(() -> IOUtils.closeQuietly(closer)));
    dir.mkdirs();

    List<File> allFiles = new ArrayList<>();
    try (ZipArchiveInputStream zinput = new ZipArchiveInputStream(zipFileInput)) {
      ZipArchiveEntry zipEntry;
      while ((zipEntry = zinput.getNextZipEntry()) != null) {
        String name = zipEntry.getName();
        File f = new File(dir, name);
        if (zipEntry.isDirectory()) {
          f.mkdirs();
        } else {
          f.getParentFile().mkdirs();
          try (FileOutputStream out = new FileOutputStream(f)) {
            IOUtils.copy(zinput, out);
          }
          allFiles.add(f);
        }
      }
    }
    return new FileClassLoader(allFiles);
  }
}
