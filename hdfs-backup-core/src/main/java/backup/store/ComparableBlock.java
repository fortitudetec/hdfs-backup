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

import org.apache.hadoop.io.WritableComparable;

public class ComparableBlock implements WritableComparable<ComparableBlock> {

  private long blkid;
  private long len;
  private long genstamp;

  public ComparableBlock() {
  }

  public ComparableBlock(long blkid, long len, long genstamp) {
    this.blkid = blkid;
    this.len = len;
    this.genstamp = genstamp;
  }

  public ExtendedBlock getExtendedBlock(String blockPoolId) {
    return new ExtendedBlock(blockPoolId, blkid, len, genstamp);
  }

  @Override
  public int compareTo(ComparableBlock o) {
    return Long.compare(blkid, o.blkid);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(getBlkid());
    out.writeLong(getLen());
    out.writeLong(getGenstamp());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setBlkid(in.readLong());
    setLen(in.readLong());
    setGenstamp(in.readLong());
  }

  public long getBlkid() {
    return blkid;
  }

  public void setBlkid(long blkid) {
    this.blkid = blkid;
  }

  public long getLen() {
    return len;
  }

  public void setLen(long len) {
    this.len = len;
  }

  public long getGenstamp() {
    return genstamp;
  }

  public void setGenstamp(long genstamp) {
    this.genstamp = genstamp;
  }

}
