package backup.datanode.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BackupStats implements Writable {

  private int backupsInProgressCount;
  private double backupBytesPerSecond;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(backupsInProgressCount);
    out.writeDouble(backupBytesPerSecond);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    backupsInProgressCount = in.readInt();
    backupBytesPerSecond = in.readDouble();
  }

  public int getBackupsInProgressCount() {
    return backupsInProgressCount;
  }

  public void setBackupsInProgressCount(int backupsInProgressCount) {
    this.backupsInProgressCount = backupsInProgressCount;
  }

  public double getBackupBytesPerSecond() {
    return backupBytesPerSecond;
  }

  public void setBackupBytesPerSecond(double backupBytesPerSecond) {
    this.backupBytesPerSecond = backupBytesPerSecond;
  }

}
