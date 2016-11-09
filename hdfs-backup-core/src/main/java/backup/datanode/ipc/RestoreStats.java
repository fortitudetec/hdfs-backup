package backup.datanode.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RestoreStats implements Writable {

  private int restoreBlocks;
  private int restoresInProgressCount;
  private double restoreBytesPerSecond;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(restoreBlocks);
    out.writeInt(restoresInProgressCount);
    out.writeDouble(restoreBytesPerSecond);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    restoreBlocks = in.readInt();
    restoresInProgressCount = in.readInt();
    restoreBytesPerSecond = in.readDouble();
  }

  public int getRestoreBlocks() {
    return restoreBlocks;
  }

  public void setRestoreBlocks(int restoreBlocks) {
    this.restoreBlocks = restoreBlocks;
  }

  public int getRestoresInProgressCount() {
    return restoresInProgressCount;
  }

  public void setRestoresInProgressCount(int restoresInProgressCount) {
    this.restoresInProgressCount = restoresInProgressCount;
  }

  public double getRestoreBytesPerSecond() {
    return restoreBytesPerSecond;
  }

  public void setRestoreBytesPerSecond(double restoreBytesPerSecond) {
    this.restoreBytesPerSecond = restoreBytesPerSecond;
  }

}
