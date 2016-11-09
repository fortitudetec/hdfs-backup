package backup.namenode.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import backup.datanode.ipc.BackupStats;
import backup.datanode.ipc.RestoreStats;

public class Stats implements Writable {

  private int finializedBlocksSizeCount;
  private int futureChecksSizeCount;
  private int backupsInProgressCount;
  private int restoreBlocks;
  private int restoresInProgressCount;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(finializedBlocksSizeCount);
    out.writeInt(futureChecksSizeCount);
    out.writeInt(backupsInProgressCount);
    out.writeInt(restoreBlocks);
    out.writeInt(restoresInProgressCount);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    finializedBlocksSizeCount = in.readInt();
    futureChecksSizeCount = in.readInt();
    backupsInProgressCount = in.readInt();
    restoreBlocks = in.readInt();
    restoresInProgressCount = in.readInt();
  }

  public void add(BackupStats backupStats) {
    finializedBlocksSizeCount += backupStats.getFinializedBlocksSizeCount();
    futureChecksSizeCount += backupStats.getFutureChecksSizeCount();
    backupsInProgressCount += backupStats.getBackupsInProgressCount();
  }

  public void add(RestoreStats restoreStats) {
    restoreBlocks += restoreStats.getRestoreBlocks();
    restoresInProgressCount += restoreStats.getRestoresInProgressCount();
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

  @Override
  public String toString() {
    return "Stats [finializedBlocksSizeCount=" + finializedBlocksSizeCount + ", futureChecksSizeCount="
        + futureChecksSizeCount + ", backupsInProgressCount=" + backupsInProgressCount + ", restoreBlocks="
        + restoreBlocks + ", restoresInProgressCount=" + restoresInProgressCount + "]";
  }

}
