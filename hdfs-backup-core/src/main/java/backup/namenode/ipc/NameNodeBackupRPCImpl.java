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

  public NameNodeBackupRPCImpl(BlockManager blockManager) {
    _blockManager = blockManager;
  }

  @Override
  public DatanodeUuids getDatanodeUuids(long blockId) throws IOException {
    DatanodeUuids datanodeUuids = new DatanodeUuids();
    Block b = new Block(blockId);
    BlockCollection blockCollection;
    try {
      blockCollection = _blockManager.getBlockCollection(b);
    } catch (NullPointerException e) {
      return datanodeUuids;
    }
    if (blockCollection == null) {
      LOGGER.info("block collection null for block id {}", blockId);
      return datanodeUuids;
    }

    BlockInfo blockInfo = blockCollection.getLastBlock();
    if (blockInfo == null) {
      LOGGER.info("last block info null for block id {}", blockId);
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
