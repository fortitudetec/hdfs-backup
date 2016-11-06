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

import java.io.IOException;

import backup.store.WritableExtendedBlock;

public class DataNodeBackupRPCImpl implements DataNodeBackupRPC {

  private final DataNodeBackupProcessor backupProcessor;
  private final DataNodeRestoreProcessor restoreProcessor;

  public DataNodeBackupRPCImpl(DataNodeBackupProcessor backupProcessor, DataNodeRestoreProcessor restoreProcessor) {
    this.backupProcessor = backupProcessor;
    this.restoreProcessor = restoreProcessor;
  }

  @Override
  public void backupBlock(WritableExtendedBlock extendedBlock) throws IOException {
    backupProcessor.addToBackupQueue(extendedBlock);
  }

  @Override
  public void restoreBlock(WritableExtendedBlock extendedBlock) throws IOException {
    restoreProcessor.addToRestoreQueue(extendedBlock);
  }

}
