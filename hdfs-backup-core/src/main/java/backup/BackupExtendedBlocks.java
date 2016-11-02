package backup;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

public class BackupExtendedBlocks {

  @JsonIgnore
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @JsonIgnore
  public static List<ExtendedBlock> toBackupExtendedBlocks(byte[] bs) throws Exception {
    BackupExtendedBlocks backupExtendedBlocks = MAPPER.readValue(bs, BackupExtendedBlocks.class);
    return backupExtendedBlocks.getExtendedBlocks();
  }

  @JsonIgnore
  public static byte[] toBytes(List<ExtendedBlock> extendedBlocks) throws Exception {
    BackupExtendedBlocks backupExtendedBlocks = new BackupExtendedBlocks(toBackupExtendedBlocks(extendedBlocks));
    return MAPPER.writeValueAsBytes(backupExtendedBlocks);
  }

  @JsonIgnore
  private static List<BackupExtendedBlock> toBackupExtendedBlocks(List<ExtendedBlock> extendedBlocks) {
    List<BackupExtendedBlock> list = new ArrayList<>();
    for (ExtendedBlock extendedBlock : extendedBlocks) {
      list.add(new BackupExtendedBlock(extendedBlock));
    }
    return list;
  }

  private final List<BackupExtendedBlock> blocks;

  @JsonCreator
  public BackupExtendedBlocks(@JsonProperty("blocks") List<BackupExtendedBlock> blocks) {
    this.blocks = blocks;
  }

  @JsonProperty
  public List<BackupExtendedBlock> getBlocks() {
    return blocks;
  }

  @JsonIgnore
  public List<ExtendedBlock> getExtendedBlocks() {
    List<ExtendedBlock> list = new ArrayList<>();
    for (BackupExtendedBlock backupExtendedBlock : blocks) {
      list.add(backupExtendedBlock.getExtendedBlock());
    }
    return list;
  }
}
