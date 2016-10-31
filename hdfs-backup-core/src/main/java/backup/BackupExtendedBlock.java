package backup;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

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
    this(extendedBlock.getBlockPoolId(), extendedBlock.getBlockId(), extendedBlock.getNumBytes(),
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
