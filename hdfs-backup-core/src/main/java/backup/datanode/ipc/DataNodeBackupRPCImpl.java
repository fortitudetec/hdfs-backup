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
package backup.datanode.ipc;

import java.io.IOException;

import backup.datanode.DataNodeBackupProcessor;
import backup.datanode.DataNodeRestoreProcessor;
import backup.store.ExtendedBlock;

public class DataNodeBackupRPCImpl implements DataNodeBackupRPC {

  private final DataNodeBackupProcessor backupProcessor;
  private final DataNodeRestoreProcessor restoreProcessor;

  public DataNodeBackupRPCImpl(DataNodeBackupProcessor backupProcessor, DataNodeRestoreProcessor restoreProcessor) {
    this.backupProcessor = backupProcessor;
    this.restoreProcessor = restoreProcessor;
  }

  @Override
  public void backupBlock(String poolId, long blockId, long length, long generationStamp) throws IOException {
    try {
      backupProcessor.performBackup(new ExtendedBlock(poolId, blockId, length, generationStamp));
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public boolean restoreBlock(String poolId, long blockId, long length, long generationStamp) throws IOException {
    try {
      return restoreProcessor.addToRestoreQueue(new ExtendedBlock(poolId, blockId, length, generationStamp));
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public BackupStats getBackupStats() throws IOException {
    return backupProcessor.getBackupStats();
  }

  @Override
  public RestoreStats getRestoreStats() throws IOException {
    return restoreProcessor.getRestoreStats();
  }

}
