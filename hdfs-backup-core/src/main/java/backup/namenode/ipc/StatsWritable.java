package backup.namenode.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.annotation.JsonProperty;

import backup.api.Stats;
import backup.datanode.ipc.BackupStats;
import backup.datanode.ipc.RestoreStats;

public class StatsWritable implements Writable, Stats {

  @JsonProperty
  private int backupsInProgressCount;
  @JsonProperty
  private double backupBytesPerSecond;
  @JsonProperty
  private int restoreBlocks;
  @JsonProperty
  private int restoresInProgressCount;
  @JsonProperty
  private double restoreBytesPerSecond;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(backupsInProgressCount);
    out.writeDouble(backupBytesPerSecond);
    out.writeInt(restoreBlocks);
    out.writeInt(restoresInProgressCount);
    out.writeDouble(restoreBytesPerSecond);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    backupsInProgressCount = in.readInt();
    backupBytesPerSecond = in.readDouble();
    restoreBlocks = in.readInt();
    restoresInProgressCount = in.readInt();
    restoreBytesPerSecond = in.readDouble();
  }

  public void add(BackupStats backupStats) {
    backupsInProgressCount += backupStats.getBackupsInProgressCount();
    backupBytesPerSecond += backupStats.getBackupBytesPerSecond();
  }

  public void add(RestoreStats restoreStats) {
    restoreBlocks += restoreStats.getRestoreBlocks();
    restoresInProgressCount += restoreStats.getRestoresInProgressCount();
    restoreBytesPerSecond += restoreStats.getRestoreBytesPerSecond();
  }

  @Override
  public int getBackupsInProgressCount() {
    return backupsInProgressCount;
  }

  public void setBackupsInProgressCount(int backupsInProgressCount) {
    this.backupsInProgressCount = backupsInProgressCount;
  }

  @Override
  public int getRestoreBlocks() {
    return restoreBlocks;
  }

  public void setRestoreBlocks(int restoreBlocks) {
    this.restoreBlocks = restoreBlocks;
  }

  @Override
  public int getRestoresInProgressCount() {
    return restoresInProgressCount;
  }

  public void setRestoresInProgressCount(int restoresInProgressCount) {
    this.restoresInProgressCount = restoresInProgressCount;
  }

  @Override
  public double getBackupBytesPerSecond() {
    return backupBytesPerSecond;
  }

  public void setBackupBytesPerSecond(double backupBytesPerSecond) {
    this.backupBytesPerSecond = backupBytesPerSecond;
  }

  @Override
  public double getRestoreBytesPerSecond() {
    return restoreBytesPerSecond;
  }

  public void setRestoreBytesPerSecond(double restoreBytesPerSecond) {
    this.restoreBytesPerSecond = restoreBytesPerSecond;
  }

  @Override
  public String toString() {
    return "StatsWritable [backupsInProgressCount=" + backupsInProgressCount + ", backupBytesPerSecond="
        + backupBytesPerSecond + ", restoreBlocks=" + restoreBlocks + ", restoresInProgressCount="
        + restoresInProgressCount + ", restoreBytesPerSecond=" + restoreBytesPerSecond + "]";
  }

}
