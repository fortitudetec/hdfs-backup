package backup.datanode;

import backup.store.ExtendedBlock;

public class ExtendedBlockEntry implements Comparable<ExtendedBlockEntry> {

  private final ExtendedBlock _extendedBlock;
  private final long _attemptBackupTimestamp;
  private final boolean _force;

  public static ExtendedBlockEntry create(ExtendedBlock extendedBlock, long age) {
    return create(extendedBlock, age, false);
  }

  public static ExtendedBlockEntry create(ExtendedBlock extendedBlock, long age, boolean force) {
    return new ExtendedBlockEntry(extendedBlock, System.currentTimeMillis() + age, force);
  }

  private ExtendedBlockEntry(ExtendedBlock extendedBlock, long attemptBackupTimestamp, boolean force) {
    _force = force;
    _extendedBlock = extendedBlock;
    _attemptBackupTimestamp = attemptBackupTimestamp;
  }

  public ExtendedBlock getExtendedBlock() {
    return _extendedBlock;
  }

  public long getAttemptBackupTimestamp() {
    return _attemptBackupTimestamp;
  }

  public boolean shouldProcess() {
    return getAttemptBackupTimestamp() <= System.currentTimeMillis();
  }

  public boolean isForce() {
    return _force;
  }

  @Override
  public int compareTo(ExtendedBlockEntry o) {
    return Long.compare(_attemptBackupTimestamp, o._attemptBackupTimestamp);
  }

  @Override
  public String toString() {
    return "ExtendedBlockEntry [_extendedBlock=" + _extendedBlock + ", _attemptBackupTimestamp="
        + _attemptBackupTimestamp + ", _force=" + _force + "]";
  }

}