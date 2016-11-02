package backup.store;

import static backup.BackupConstants.DFS_BACKUP_STORE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_STORE_KEY;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class BackupStore extends Configured {

  public static BackupStore create(Configuration conf) throws Exception {
    Class<? extends BackupStore> clazz = conf.getClass(DFS_BACKUP_STORE_KEY, DFS_BACKUP_STORE_DEFAULT,
        BackupStore.class);
    BackupStore backupStore = ReflectionUtils.newInstance(clazz, conf);
    backupStore.init();
    return backupStore;
  }

  public abstract void init() throws Exception;

  /**
   * The backup method will need to store the extendedBlock, data stream, and
   * metaData stream. The input streams will be exhausted after this method
   * succeeds but the streams will not be closed that will be the responsibility
   * of the calling class.
   */
  public abstract void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData)
      throws Exception;

  /**
   * Check to see if backup store has extendedBlock.
   */
  public abstract boolean hasBlock(ExtendedBlock extendedBlock) throws Exception;

  /**
   * Read the meta data stream from backup store for given block.
   */
  public abstract InputStream getMetaDataInputStream(ExtendedBlock extendedBlock) throws Exception;

  /**
   * Read the data stream from backup store for given block.
   */
  public abstract InputStream getDataInputStream(ExtendedBlock extendedBlock) throws Exception;

  public abstract ExtendedBlockEnum getExtendedBlocks() throws Exception;

  /**
   * Removes block from the backup store.
   */
  public abstract void deleteBlock(ExtendedBlock extendedBlock) throws Exception;
}
