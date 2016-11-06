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
    BackupUtil.writeShortString(poolId, out);
    out.writeLong(blockId);
    out.writeLong(length);
    out.writeLong(generationStamp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    poolId = BackupUtil.readShortString(in);
    blockId = in.readLong();
    length = in.readLong();
    generationStamp = in.readLong();
  }

}
