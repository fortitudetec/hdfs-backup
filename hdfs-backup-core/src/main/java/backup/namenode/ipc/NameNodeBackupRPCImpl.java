package backup.namenode.ipc;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.datanode.ipc.DataNodeBackupRPC;
import backup.namenode.NameNodeRestoreProcessor;

public class NameNodeBackupRPCImpl implements NameNodeBackupRPC {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupRPCImpl.class);

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
      try {
        DataNodeBackupRPC backup = DataNodeBackupRPC.getDataNodeBackupRPC(datanodeInfo, conf);
        stats.add(backup.getBackupStats());
        stats.add(backup.getRestoreStats());
      } catch (Exception e) {
        LOG.error("Error while trying to read hdfs backup stats from datanode {}", datanodeInfo.getHostName());
      }
    }
    return stats;
  }

}
