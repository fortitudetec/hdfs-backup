package backup.store;

import java.io.InputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

public abstract class BackupStore extends Configured {

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

}
