package backup.namenode.ipc;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameNodeBackupRPCImpl implements NameNodeBackupRPC {

  private static final Logger LOGGER = LoggerFactory.getLogger(NameNodeBackupRPCImpl.class);

  private final BlockManager _blockManager;

  private final String _blockPoolId;

  public NameNodeBackupRPCImpl(String blockPoolId, BlockManager blockManager) {
    _blockPoolId = blockPoolId;
    _blockManager = blockManager;
  }

  @Override
  public DatanodeUuids getDatanodeUuids(String poolId, Block block) throws IOException {
    if (!_blockPoolId.equals(poolId)) {
      throw new IOException("Block pool id " + poolId + " does not match this namenode " + _blockPoolId);
    }
    DatanodeUuids datanodeUuids = new DatanodeUuids();
    BlockCollection blockCollection;
    try {
      blockCollection = _blockManager.getBlockCollection(block);
    } catch (NullPointerException e) {
      return datanodeUuids;
    }
    if (blockCollection == null) {
      LOGGER.debug("block collection null for block {}", block);
      return datanodeUuids;
    }

    BlockInfo blockInfo = blockCollection.getLastBlock();
    if (blockInfo == null) {
      LOGGER.debug("last block info null for block {}", block);
      return datanodeUuids;
    }

    int numNodes = blockInfo.numNodes();
    for (int i = 0; i < numNodes; i++) {
      DatanodeDescriptor datanode = blockInfo.getDatanode(i);
      datanodeUuids.add(datanode.getDatanodeUuid());
    }
    return datanodeUuids;
  }

}
