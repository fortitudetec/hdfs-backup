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

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.ServicePlugin;

import backup.SingletonManager;

public class DataNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private DataNodeBackupProcessor backupProcessor;
  private DataNodeRestoreProcessor restoreProcessor;

  @Override
  public void start(Object service) {
    DataNode datanode = (DataNode) service;
    // This object is created here so that it's lifecycle follows the datanode
    try {
      backupProcessor = SingletonManager.getManager(DataNodeBackupProcessor.class).getInstance(datanode,
          () -> new DataNodeBackupProcessor(getConf(), datanode));
      restoreProcessor = SingletonManager.getManager(DataNodeRestoreProcessor.class).getInstance(datanode,
          () -> new DataNodeRestoreProcessor(getConf(), datanode));
      DataNodeBackupRPCImpl backupRPCImpl = new DataNodeBackupRPCImpl(backupProcessor, restoreProcessor);
      datanode.ipcServer.addProtocol(RPC.RpcKind.RPC_WRITABLE, DataNodeBackupRPC.class, backupRPCImpl);
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
