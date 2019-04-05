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
package backup.namenode.ipc;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_RPC_PORT_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.UserGroupInformation;

@KerberosInfo(serverPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName = "backup.namenode.ipc.NameNodeBackupRPC", protocolVersion = 1)
public interface NameNodeBackupRPC {

  public static NameNodeBackupRPC getDataNodeBackupRPC(InetSocketAddress nameNodeIPCAddres, Configuration conf,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    int port = conf.getInt(DFS_BACKUP_NAMENODE_RPC_PORT_KEY, DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT);
    if (port == 0) {
      port = nameNodeIPCAddres.getPort() + 1;
    }
    InetSocketAddress nameNodeAddress = new InetSocketAddress(nameNodeIPCAddres.getAddress(), port);
    return RPC.getProtocolProxy(NameNodeBackupRPC.class, RPC.getProtocolVersion(NameNodeBackupRPC.class),
        nameNodeAddress, ugi, conf, NetUtils.getDefaultSocketFactory(conf))
              .getProxy();
  }

  DatanodeUuids getDatanodeUuids(String poolId, Block block) throws IOException;
}
