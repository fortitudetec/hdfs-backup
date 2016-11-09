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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.util.ServicePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.SingletonManager;
import backup.datanode.ipc.DataNodeBackupRPC;
import backup.namenode.ipc.NameNodeBackupRPC;
import backup.namenode.ipc.NameNodeBackupRPCImpl;

public class NameNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupServicePlugin.class);

  private NameNodeRestoreProcessor restoreProcessor;
  private Server server;

  private NameNodeStatusServer httpServer;

  @Override
  public void start(Object service) {
    NameNode namenode = (NameNode) service;
    RPC.setProtocolEngine(getConf(), DataNodeBackupRPC.class, WritableRpcEngine.class);
    RPC.setProtocolEngine(getConf(), NameNodeBackupRPC.class, WritableRpcEngine.class);
    // This object is created here so that it's lifecycle follows the namenode
    try {
      restoreProcessor = SingletonManager.getManager(NameNodeRestoreProcessor.class)
                                         .getInstance(namenode,
                                             () -> new NameNodeRestoreProcessor(getConf(), namenode));

      InetSocketAddress listenerAddress = namenode.getServiceRpcAddress();
      int ipcPort = listenerAddress.getPort();
      String bindAddress = listenerAddress.getAddress()
                                          .getHostAddress();
      int port = getConf().getInt(DFS_BACKUP_NAMENODE_RPC_PORT_KEY, DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT);
      if (port == 0) {
        port = ipcPort + 1;
      }
      NameNodeBackupRPC nodeBackupRPCImpl = new NameNodeBackupRPCImpl(getConf(), namenode, restoreProcessor);

      server = new RPC.Builder(getConf()).setBindAddress(bindAddress)
                                         .setPort(port)
                                         .setInstance(nodeBackupRPCImpl)
                                         .setProtocol(NameNodeBackupRPC.class)
                                         .build();
      server.start();
      LOG.info("NameNode Backup RPC listening on {}", port);

      int httpPort = getConf().getInt(DFS_BACKUP_NAMENODE_HTTP_PORT_KEY, DFS_BACKUP_NAMENODE_HTTP_PORT_DEFAULT);
      if (httpPort != 0) {
        LOG.info("NameNode Backup HTTP listening on {}", httpPort);
        httpServer = new NameNodeStatusServer(nodeBackupRPCImpl, httpPort);
        httpServer.start();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop();
    }
    IOUtils.closeQuietly(httpServer);
    IOUtils.closeQuietly(restoreProcessor);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

}
