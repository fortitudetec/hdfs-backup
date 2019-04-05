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
package backup.namenode;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_DELETE_BATCH_SIZE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_DELETE_BATCH_SIZE_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.BaseProcessor;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.util.HdfsUtils;

public class NameNodeBackupGC extends BaseProcessor {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupGC.class);

  private final long _checkInterval;
  private final long _initInterval;
  private final Configuration _conf;
  private final BackupStore _backupStore;
  private final BlockManager _blockManager;
  private final int _maxDeleteBatch;

  public NameNodeBackupGC(Configuration conf, NameNode namenode) throws Exception {
    _conf = conf;
    _blockManager = namenode.getNamesystem()
                            .getBlockManager();
    _backupStore = BackupStore.create(BackupUtil.convert(conf));
    _checkInterval = conf.getLong(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT);
    _initInterval = conf.getLong(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT);
    _maxDeleteBatch = conf.getInt(DFS_BACKUP_NAMENODE_BLOCK_CHECK_DELETE_BATCH_SIZE_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_DELETE_BATCH_SIZE_DEFAULT);
    start();
  }

  public synchronized void runGcBlocking(boolean debug) throws Exception {
    if (!HdfsUtils.isActiveNamenode(_conf)) {
      return;
    }
    ExtendedBlock start = null;
    long startTime = System.nanoTime();
    long backupStoreCount = 0;
    long backupStoreSize = 0;
    long deleteCount = 0;
    long deleteSize = 0;
    long errorCount = 0;
    List<ExtendedBlock> batch = new ArrayList<>(_maxDeleteBatch);
    while (running.get()) {
      try {
        long nowTime = System.nanoTime();
        if (startTime + TimeUnit.SECONDS.toNanos(10) < nowTime) {
          LOG.info("Total backup store blocks processed {} ({} bytes) total deletes {} ({} bytes) total errors {}",
              backupStoreCount, backupStoreSize, deleteCount, deleteSize, errorCount);
          startTime = System.nanoTime();
        }
        LOG.info("Fetching extendedblocks starting with {}", start);
        List<ExtendedBlock> extendedBlocks = _backupStore.getExtendedBlocks(start);
        if (extendedBlocks.isEmpty()) {
          return;
        }

        // setup for next batch incase this one fails
        Collections.sort(extendedBlocks);
        start = extendedBlocks.get(extendedBlocks.size() - 1);

        backupStoreCount += extendedBlocks.size();
        backupStoreSize += getTotalBytes(extendedBlocks);
        List<ExtendedBlock> deletes = checkIfValidHdfsBlocksReturnInvalidBlocks(extendedBlocks);
        if (!deletes.isEmpty()) {
          deleteSize += getTotalBytes(deletes);
          deleteCount += deletes.size();
          LOG.info("Extendedblock from backup store need to be removed {}", deletes.size());
          if (LOG.isDebugEnabled()) {
            LOG.info("Extendedblock from backup store need to be removed {}", deletes);
          }
          performDeletes(batch, deletes);
        }

      } catch (Exception e) {
        errorCount++;
        LOG.error("Unknown error, backing off and retrying", e);
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      }
    }
    LOG.info("Finished - Total backup store blocks processed {} ({} bytes) total deletes {} ({} bytes) total errors {}",
        backupStoreCount, backupStoreSize, deleteCount, deleteSize, errorCount);
  }

  private void performDeletes(List<ExtendedBlock> batch, List<ExtendedBlock> deletes) throws Exception {
    batch.clear();
    for (ExtendedBlock extendedBlock : deletes) {
      batch.add(extendedBlock);
      if (batch.size() >= _maxDeleteBatch) {
        _backupStore.deleteBlocks(batch);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      _backupStore.deleteBlocks(batch);
      batch.clear();
    }
  }

  private long getTotalBytes(List<ExtendedBlock> extendedBlocks) {
    long total = 0;
    for (ExtendedBlock extendedBlock : extendedBlocks) {
      total += extendedBlock.getLength();
    }
    return total;
  }

  private List<ExtendedBlock> checkIfValidHdfsBlocksReturnInvalidBlocks(List<ExtendedBlock> extendedBlocks) {
    List<ExtendedBlock> deletes = new ArrayList<>();
    for (ExtendedBlock extendedBlock : extendedBlocks) {
      if (!isValidHdfsBlock(extendedBlock)) {
        deletes.add(extendedBlock);
      }
    }
    return deletes;
  }

  private boolean isValidHdfsBlock(ExtendedBlock extendedBlock) {
    Block block = new Block(extendedBlock.getBlockId(), extendedBlock.getLength(), extendedBlock.getGenerationStamp());
    try {
      if (_blockManager.getBlockCollection(block) == null) {
        return false;
      }
    } catch (NullPointerException e) {
      return false;
    }
    return true;
  }

  @Override
  protected void initInternal() throws Exception {
    Thread.sleep(_initInterval);
  }

  @Override
  protected void closeInternal() {
    IOUtils.closeQuietly(_backupStore);
  }

  @Override
  protected void runInternal() throws Exception {
    try {
      runGcBlocking(false);
    } catch (Throwable t) {
      LOG.error("Unknown error during block check", t);
      throw t;
    } finally {
      Thread.sleep(_checkInterval);
    }
  }

  public void runGc(boolean debug) {
    Thread thread = new Thread(() -> {
      try {
        runGcBlocking(debug);
      } catch (Exception e) {
        LOG.error("Unknown error", e);
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

}
