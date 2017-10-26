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
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import backup.api.StatsService;

@ProtocolInfo(protocolName = "NameNodeBackupRPC", protocolVersion = 1)
public interface NameNodeBackupRPC extends StatsService<StatsWritable> {

  public static NameNodeBackupRPC getNameNodeBackupRPC(String host, Configuration conf, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    int port = conf.getInt(DFS_BACKUP_NAMENODE_RPC_PORT_KEY, DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT);
    if (port == 0) {
      throw new RuntimeException("Can not determine port.");
    }
    return RPC.getProtocolProxy(NameNodeBackupRPC.class, RPC.getProtocolVersion(NameNodeBackupRPC.class),
        new InetSocketAddress(host, port), ugi, conf, NetUtils.getDefaultSocketFactory(conf))
              .getProxy();
  }

  public static NameNodeBackupRPC getNameNodeBackupRPC(InetSocketAddress nameNodeAddress, Configuration conf,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    int port = conf.getInt(DFS_BACKUP_NAMENODE_RPC_PORT_KEY, DFS_BACKUP_NAMENODE_RPC_PORT_DEFAULT);
    if (port == 0) {
      port = nameNodeAddress.getPort() + 1;
    }
    return RPC.getProtocolProxy(NameNodeBackupRPC.class, RPC.getProtocolVersion(NameNodeBackupRPC.class),
        nameNodeAddress, ugi, conf, NetUtils.getDefaultSocketFactory(conf))
              .getProxy();
  }

  void backupBlock(String poolId, long blockId, long length, long generationStamp) throws IOException;

  void restoreBlock(String poolId, long blockId, long length, long generationStamp) throws IOException;

}
