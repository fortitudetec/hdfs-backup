package backup.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WritableExtendedBlock implements Writable {

  private String poolId;
  private long blockId;
  private long length;
  private long generationStamp;

  public WritableExtendedBlock() {

  }

  public WritableExtendedBlock(ExtendedBlock extendedBlock) {
    this(extendedBlock.getPoolId(), extendedBlock.getBlockId(), extendedBlock.getLength(),
        extendedBlock.getGenerationStamp());
  }

  public WritableExtendedBlock(String poolId, long blockId, long length, long generationStamp) {
    this.poolId = poolId;
    this.blockId = blockId;
    this.length = length;
    this.generationStamp = generationStamp;
  }

  public ExtendedBlock getExtendedBlock() {
    return new ExtendedBlock(poolId, blockId, length, generationStamp);
  }

  public String getPoolId() {
    return poolId;
  }

  public void setPoolId(String poolId) {
    this.poolId = poolId;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStamp(long generationStamp) {
    this.generationStamp = generationStamp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtil.writeShortString(poolId, out);
    out.writeLong(blockId);
    out.writeLong(length);
    out.writeLong(generationStamp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    poolId = WritableUtil.readShortString(in);
    blockId = in.readLong();
    length = in.readLong();
    generationStamp = in.readLong();
  }

}
