package backup.datanode.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BackupStats implements Writable {

  private int finializedBlocksSizeCount;
  private int futureChecksSizeCount;
  private int backupsInProgressCount;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(finializedBlocksSizeCount);
    out.writeInt(futureChecksSizeCount);
    out.writeInt(backupsInProgressCount);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    finializedBlocksSizeCount = in.readInt();
    futureChecksSizeCount = in.readInt();
    backupsInProgressCount = in.readInt();
  }

  public int getFinializedBlocksSizeCount() {
    return finializedBlocksSizeCount;
  }

  public void setFinializedBlocksSizeCount(int finializedBlocksSizeCount) {
    this.finializedBlocksSizeCount = finializedBlocksSizeCount;
  }

  public int getFutureChecksSizeCount() {
    return futureChecksSizeCount;
  }

  public void setFutureChecksSizeCount(int futureChecksSizeCount) {
    this.futureChecksSizeCount = futureChecksSizeCount;
  }

  public int getBackupsInProgressCount() {
    return backupsInProgressCount;
  }

  public void setBackupsInProgressCount(int backupsInProgressCount) {
    this.backupsInProgressCount = backupsInProgressCount;
  }

}
