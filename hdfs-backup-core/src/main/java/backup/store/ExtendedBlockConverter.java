package backup.store;

import backup.store.ExtendedBlock;

public class ExtendedBlockConverter {
  public static org.apache.hadoop.hdfs.protocol.ExtendedBlock toHadoop(ExtendedBlock block) {
    if (block == null) {
      return null;
    }
    return new org.apache.hadoop.hdfs.protocol.ExtendedBlock(block.getPoolId(), block.getBlockId(), block.getLength(),
        block.getGenerationStamp());
  }

  public static ExtendedBlock fromHadoop(org.apache.hadoop.hdfs.protocol.ExtendedBlock block) {
    if (block == null) {
      return null;
    }
    return new ExtendedBlock(block.getBlockPoolId(), block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp());
  }
}
