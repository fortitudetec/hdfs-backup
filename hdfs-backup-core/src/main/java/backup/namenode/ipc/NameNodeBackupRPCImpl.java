package backup.namenode.ipc;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import backup.datanode.ipc.DataNodeBackupRPC;
import backup.namenode.NameNodeRestoreProcessor;

public class NameNodeBackupRPCImpl implements NameNodeBackupRPC {

  private final NameNodeRestoreProcessor restoreProcessor;
  private final BlockManager blockManager;
  private final Configuration conf;

  public NameNodeBackupRPCImpl(Configuration conf, NameNode nameNode, NameNodeRestoreProcessor restoreProcessor) {
    this.conf = conf;
    this.restoreProcessor = restoreProcessor;
    this.blockManager = nameNode.getNamesystem()
                                .getBlockManager();
  }

  @Override
  public void backupBlock(String poolId, long blockId, long length, long generationStamp) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void restoreBlock(String poolId, long blockId, long length, long generationStamp) throws IOException {
    restoreProcessor.restoreBlock(poolId, blockId, length, generationStamp);
  }

  @Override
  public Stats getStats() throws IOException {
    Stats stats = new Stats();
    Set<DatanodeDescriptor> datanodes = blockManager.getDatanodeManager()
                                                    .getDatanodes();
    for (DatanodeInfo datanodeInfo : datanodes) {
      DataNodeBackupRPC backup = DataNodeBackupRPC.getDataNodeBackupRPC(datanodeInfo, conf);
      stats.add(backup.getBackupStats());
      stats.add(backup.getRestoreStats());
    }
    return stats;
  }

}
