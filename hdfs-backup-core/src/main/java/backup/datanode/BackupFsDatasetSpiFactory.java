/*
 * Copyright 2016 Fortitude Technologies LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import backup.SingletonManager;
import backup.store.BackupUtil;

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
      backupProcessor = SingletonManager.getManager(DataNodeBackupProcessor.class)
                                        .getInstance(datanode, () -> new DataNodeBackupProcessor(conf, datanode));
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
        String name = method.getName();
        if (name.equals(FINALIZE_BLOCK)) {
          ExtendedBlock extendedBlock = (ExtendedBlock) args[0];
          backup.store.ExtendedBlock eb = BackupUtil.fromHadoop(extendedBlock);
          backupProcessor.blockFinalized(false, eb);
        }
        return result;
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }
}
