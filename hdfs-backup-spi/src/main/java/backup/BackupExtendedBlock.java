package backup;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import backup.store.ExtendedBlock;

public class BackupExtendedBlock {
  @JsonIgnore
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @JsonIgnore
  public static ExtendedBlock toBackupExtendedBlock(byte[] bs) throws Exception {
    BackupExtendedBlock backupExtendedBlock = MAPPER.readValue(bs, BackupExtendedBlock.class);
    return backupExtendedBlock.getExtendedBlock();
  }

  @JsonIgnore
  public static byte[] toBytes(ExtendedBlock extendedBlock) throws Exception {
    BackupExtendedBlock backupExtendedBlock = new BackupExtendedBlock(extendedBlock);
    return MAPPER.writeValueAsBytes(backupExtendedBlock);
  }

  private final String poolId;
  private final long blkid;
  private final long len;
  private final long genstamp;

  @JsonCreator
  public BackupExtendedBlock(@JsonProperty("poolId") String poolId, @JsonProperty("blkid") long blkid,
      @JsonProperty("len") long len, @JsonProperty("genstamp") long genstamp) {
    this.poolId = poolId;
    this.blkid = blkid;
    this.len = len;
    this.genstamp = genstamp;
  }

  @JsonIgnore
  public BackupExtendedBlock(ExtendedBlock extendedBlock) {
    this(extendedBlock.getPoolId(), extendedBlock.getBlockId(), extendedBlock.getLength(),
        extendedBlock.getGenerationStamp());
  }

  @JsonIgnore
  public ExtendedBlock getExtendedBlock() {
    return new ExtendedBlock(poolId, blkid, len, genstamp);
  }

  @JsonProperty
  public String getPoolId() {
    return poolId;
  }

  @JsonProperty
  public long getBlkid() {
    return blkid;
  }

  @JsonProperty
  public long getLen() {
    return len;
  }

  @JsonProperty
  public long getGenstamp() {
    return genstamp;
  }

}
