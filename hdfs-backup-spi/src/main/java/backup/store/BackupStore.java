package backup.store;

import static backup.BackupConstants.DFS_BACKUP_STORE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_STORE_KEY;

import java.io.InputStream;

import org.apache.commons.configuration.Configuration;

public abstract class BackupStore extends Configured {

  @SuppressWarnings("unchecked")
  public synchronized static BackupStore create(Configuration conf) throws Exception {
    Class<? extends BackupStore> clazz;
    try {
      String classname = conf.getString(DFS_BACKUP_STORE_KEY, DFS_BACKUP_STORE_DEFAULT);
      clazz = (Class<? extends BackupStore>) BackupStore.class.getClassLoader()
                                                              .loadClass(classname);
    } catch (Exception e) {
      String classname = conf.getString(DFS_BACKUP_STORE_KEY);
      clazz = (Class<? extends BackupStore>) BackupStoreClassHelper.tryToFindPlugin(classname);
    }
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

  public abstract ExtendedBlockEnum<Void> getExtendedBlocks() throws Exception;

  /**
   * Removes block from the backup store.
   */
  public abstract void deleteBlock(ExtendedBlock extendedBlock) throws Exception;
}
