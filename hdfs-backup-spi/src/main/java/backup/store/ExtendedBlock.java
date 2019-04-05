package backup.store;

/**
 * Identifies a Block uniquely across the block pools
 */
public class ExtendedBlock implements Comparable<ExtendedBlock> {
  private final String poolId;
  private final long blockId;
  private final long length;
  private final long generationStamp;

  public ExtendedBlock(ExtendedBlock extendedBlock) {
    this.poolId = extendedBlock.getPoolId();
    this.blockId = extendedBlock.getBlockId();
    this.length = extendedBlock.getLength();
    this.generationStamp = extendedBlock.getGenerationStamp();
  }

  public ExtendedBlock(String poolId, long blockId, long length, long generationStamp) {
    this.poolId = poolId;
    this.blockId = blockId;
    this.length = length;
    this.generationStamp = generationStamp;
  }

  public String getPoolId() {
    return poolId;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getLength() {
    return length;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  @Override
  public String toString() {
    return "ExtendedBlock [poolId=" + poolId + ", blockId=" + blockId + ", length=" + length + ", generationStamp="
        + generationStamp + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (blockId ^ (blockId >>> 32));
    result = prime * result + (int) (generationStamp ^ (generationStamp >>> 32));
    result = prime * result + (int) (length ^ (length >>> 32));
    result = prime * result + ((poolId == null) ? 0 : poolId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ExtendedBlock other = (ExtendedBlock) obj;
    if (blockId != other.blockId)
      return false;
    if (generationStamp != other.generationStamp)
      return false;
    if (length != other.length)
      return false;
    if (poolId == null) {
      if (other.poolId != null)
        return false;
    } else if (!poolId.equals(other.poolId))
      return false;
    return true;
  }

  @Override
  public int compareTo(ExtendedBlock o) {
    int compare = poolId.compareTo(o.poolId);
    if (compare != 0) {
      compare = Long.compare(blockId, o.blockId);
      if (compare != 0) {
        return Long.compare(generationStamp, o.generationStamp);
      }
    }
    return compare;
  }

}
