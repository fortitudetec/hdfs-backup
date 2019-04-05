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
package backup.datanode.ipc;

import static backup.BackupConstants.DFS_BACKUP_DATANODE_RPC_PORT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RPC_PORT_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.UserGroupInformation;

@KerberosInfo(serverPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName = "backup.datanode.ipc.DataNodeBackupRPC", protocolVersion = 1)
public interface DataNodeBackupRPC {

  public static DataNodeBackupRPC getDataNodeBackupRPC(DatanodeInfo datanodeInfo, Configuration conf,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    String ipcHostname = datanodeInfo.getHostName();
    int ipcPort = datanodeInfo.getIpcPort();
    InetSocketAddress dataNodeIPCAddress = new InetSocketAddress(ipcHostname, ipcPort);
    return getDataNodeBackupRPC(dataNodeIPCAddress, conf, ugi);
  }

  public static DataNodeBackupRPC getDataNodeBackupRPC(InetSocketAddress dataNodeIPCAddress, Configuration conf,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    int port = conf.getInt(DFS_BACKUP_DATANODE_RPC_PORT_KEY, DFS_BACKUP_DATANODE_RPC_PORT_DEFAULT);
    if (port == 0) {
      port = dataNodeIPCAddress.getPort() + 1;
    }
    InetSocketAddress dataNodeAddress = new InetSocketAddress(dataNodeIPCAddress.getAddress(), port);
    return RPC.getProtocolProxy(DataNodeBackupRPC.class, RPC.getProtocolVersion(DataNodeBackupRPC.class),
        dataNodeAddress, ugi, conf, NetUtils.getDefaultSocketFactory(conf))
              .getProxy();
  }

  /**
   * Backup a single block. This is a blocking call.
   * 
   * @param poolId
   * @param blockId
   * @param length
   * @param generationStamp
   * @throws IOException
   */
  void backupBlock(String poolId, long blockId, long length, long generationStamp) throws IOException;

  /**
   * Restores a single block. This is a blocking call.
   * 
   * @param poolId
   * @param blockId
   * @param length
   * @param generationStamp
   * @return
   * @throws IOException
   */
  boolean restoreBlock(String poolId, long blockId, long length, long generationStamp) throws IOException;

  /**
   * Get backup stats.
   * 
   * @return
   * @throws IOException
   */
  BackupStats getBackupStats() throws IOException;

  /**
   * Get restore stats.
   * 
   * @return
   * @throws IOException
   */
  RestoreStats getRestoreStats() throws IOException;

  void runBlockCheck(boolean blocking, boolean ignorePreviousChecks, String blockPoolId) throws IOException;

}
