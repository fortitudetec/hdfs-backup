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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;

@ProtocolInfo(protocolName = "NameNodeBackupRPC", protocolVersion = 1)
public interface NameNodeBackupRPC {

  public static NameNodeBackupRPC getDataNodeBackupRPC(InetSocketAddress nameNodeAddress, Configuration conf)
      throws IOException {
    return RPC.getProxy(NameNodeBackupRPC.class, RPC.getProtocolVersion(NameNodeBackupRPC.class), nameNodeAddress,
        conf);
  }

  void backupBlock(String poolId, long blockId, long length, long generationStamp) throws IOException;

  void restoreBlock(String poolId, long blockId, long length, long generationStamp) throws IOException;

  Stats getStats() throws IOException;

}
