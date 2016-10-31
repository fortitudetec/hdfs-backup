package backup.datanode;

import static backup.BackupConstants.DFS_DATANODE_BACKUP_FSDATASET_FACTORY_KEY;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.Factory;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BackupFsDatasetSpiFactory extends Factory<FsDatasetSpi<?>> {

  private static final String FINALIZE_BLOCK = "finalizeBlock";

  private Factory<FsDatasetSpi<?>> factory;
  private DataNodeBackupProcessor backupProcessor;

  @Override
  public FsDatasetSpi<?> newInstance(DataNode datanode, DataStorage storage, Configuration conf) throws IOException {
    setupDefaultFactory(conf);
    try {
      setupBackupProcessor(conf, datanode);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return setupBackupEvents(factory.newInstance(datanode, storage, conf), backupProcessor);
  }

  private void setupBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    if (backupProcessor == null) {
      backupProcessor = DataNodeBackupProcessor.newInstance(conf, datanode);
    }
  }

  private void setupDefaultFactory(Configuration conf) {
    if (factory == null) {
      Class<? extends Factory> defaultFactoryClass = conf.getClass(DFS_DATANODE_BACKUP_FSDATASET_FACTORY_KEY,
          FsDatasetFactory.class, Factory.class);
      factory = ReflectionUtils.newInstance(defaultFactoryClass, conf);
    }
  }

  /**
   * A proxy class is used here to allow the code to more flexible across
   * different hdfs versions.
   * 
   * @param factory
   * @return
   */
  private static FsDatasetSpi<?> setupBackupEvents(FsDatasetSpi<?> datasetSpi,
      DataNodeBackupProcessor backupProcessor) {
    InvocationHandler handler = new BackupInvocationHandler(datasetSpi, backupProcessor);
    return (FsDatasetSpi<?>) Proxy.newProxyInstance(FsDatasetSpi.class.getClassLoader(),
        new Class<?>[] { FsDatasetSpi.class }, handler);
  }

  static class BackupInvocationHandler implements InvocationHandler {

    private final FsDatasetSpi<?> datasetSpi;
    private final DataNodeBackupProcessor backupProcessor;

    public BackupInvocationHandler(FsDatasetSpi<?> datasetSpi, DataNodeBackupProcessor backupProcessor) {
      this.datasetSpi = datasetSpi;
      this.backupProcessor = backupProcessor;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        Object result = method.invoke(datasetSpi, args);
        if (method.getName().equals(FINALIZE_BLOCK)) {
          ExtendedBlock extendedBlock = (ExtendedBlock) args[0];
          backupProcessor.blockFinalized(extendedBlock);
        }
        return result;
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      }
    }
  }
}
