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
package backup.datanode;

import static backup.BackupConstants.DFS_BACKUP_DATANODE_RPC_PORT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RPC_PORT_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.util.ServicePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.SingletonManager;
import backup.datanode.ipc.DataNodeBackupRPC;
import backup.datanode.ipc.DataNodeBackupRPCImpl;
import backup.namenode.ipc.NameNodeBackupRPC;

public class DataNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeBackupServicePlugin.class);

  private DataNodeBackupProcessor backupProcessor;
  private DataNodeRestoreProcessor restoreProcessor;
  private Server server;

  @Override
  public void start(Object service) {
    DataNode datanode = (DataNode) service;
    Configuration conf = getConf();
    RPC.setProtocolEngine(conf, DataNodeBackupRPC.class, WritableRpcEngine.class);
    RPC.setProtocolEngine(conf, NameNodeBackupRPC.class, WritableRpcEngine.class);
    // This object is created here so that it's lifecycle follows the datanode
    try {
      backupProcessor = SingletonManager.getManager(DataNodeBackupProcessor.class)
                                        .getInstance(datanode, () -> new DataNodeBackupProcessor(conf, datanode));
      restoreProcessor = SingletonManager.getManager(DataNodeRestoreProcessor.class)
                                         .getInstance(datanode, () -> new DataNodeRestoreProcessor(conf, datanode));

      DataNodeBackupRPCImpl backupRPCImpl = new DataNodeBackupRPCImpl(backupProcessor, restoreProcessor);

      InetSocketAddress listenerAddress = datanode.ipcServer.getListenerAddress();
      int ipcPort = listenerAddress.getPort();
      String bindAddress = listenerAddress.getAddress()
                                          .getHostAddress();
      int port = conf.getInt(DFS_BACKUP_DATANODE_RPC_PORT_KEY, DFS_BACKUP_DATANODE_RPC_PORT_DEFAULT);
      if (port == 0) {
        port = ipcPort + 1;
      }
      server = new RPC.Builder(conf).setBindAddress(bindAddress)
                                    .setPort(port)
                                    .setInstance(backupRPCImpl)
                                    .setProtocol(DataNodeBackupRPC.class)
                                    .build();
      server.start();

      LOG.info("DataNode Backup RPC listening on {}", port);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop();
    }
    IOUtils.closeQuietly(backupProcessor);
    IOUtils.closeQuietly(restoreProcessor);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

}
