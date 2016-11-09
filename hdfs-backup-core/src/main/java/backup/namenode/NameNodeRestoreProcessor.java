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

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import backup.BaseProcessor;
import backup.datanode.ipc.DataNodeBackupRPC;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;

public class NameNodeRestoreProcessor extends BaseProcessor {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeRestoreProcessor.class);

  private final long pollTime;
  private final Set<ExtendedBlock> currentRequestedRestore;
  private final NameNodeBackupBlockCheckProcessor blockCheck;
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private final BlockManager blockManager;

  public NameNodeRestoreProcessor(Configuration conf, NameNode namenode) throws Exception {
    this.conf = conf;
    this.namesystem = namenode.getNamesystem();
    this.blockManager = namesystem.getBlockManager();
    Cache<ExtendedBlock, Boolean> cache = CacheBuilder.newBuilder()
                                                      .expireAfterWrite(10, TimeUnit.MINUTES)
                                                      .build();
    currentRequestedRestore = Collections.newSetFromMap(cache.asMap());
    pollTime = conf.getLong(DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY,
        DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT);
    blockCheck = new NameNodeBackupBlockCheckProcessor(conf, this);
    start();
  }

  @Override
  protected void closeInternal() {
    IOUtils.closeQuietly(blockCheck);
  }

  @Override
  protected void runInternal() throws Exception {
    if (!checkForBlocksToRestore()) {
      Thread.sleep(pollTime);
    }
  }

  private boolean checkForBlocksToRestore() throws Exception {
    String blockPoolId = namesystem.getBlockPoolId();
    Iterator<? extends Block> blockIterator = blockManager.getCorruptReplicaBlockIterator();
    boolean atLeastOneRestoreRequest = false;
    while (blockIterator.hasNext()) {
      Block block = blockIterator.next();
      long blockId = block.getBlockId();
      long length = block.getNumBytes();
      long generationStamp = block.getGenerationStamp();

      ExtendedBlock extendedBlock = new ExtendedBlock(blockPoolId, blockId, length, generationStamp);
      if (!hasRestoreBeenRequested(extendedBlock)) {
        LOG.info("Need to restore block {}", extendedBlock);
        requestRestore(extendedBlock);
        atLeastOneRestoreRequest = true;
      }
    }
    return atLeastOneRestoreRequest;
  }

  public synchronized void requestRestore(ExtendedBlock extendedBlock) throws Exception {
    Set<DatanodeDescriptor> datanodes = blockManager.getDatanodeManager()
                                                    .getDatanodes();
    DatanodeInfo datanodeInfo = getDataNodeAddress(datanodes);
    DataNodeBackupRPC backup = DataNodeBackupRPC.getDataNodeBackupRPC(datanodeInfo, conf);
    if (backup.restoreBlock(extendedBlock.getPoolId(), extendedBlock.getBlockId(), extendedBlock.getLength(),
        extendedBlock.getGenerationStamp())) {
      currentRequestedRestore.add(extendedBlock);
    }
  }

  private DatanodeInfo getDataNodeAddress(Set<DatanodeDescriptor> storages) {
    DatanodeInfo[] datanodeInfos = storages.toArray(new DatanodeInfo[storages.size()]);
    int index = BackupUtil.nextInt(datanodeInfos.length);
    return datanodeInfos[index];
  }

  private boolean hasRestoreBeenRequested(ExtendedBlock extendedBlock) {
    return currentRequestedRestore.contains(extendedBlock);
  }

  public void runBlockCheck() throws Exception {
    this.blockCheck.runBlockCheck();
  }

  public void restoreBlock(String poolId, long blockId, long length, long generationStamp) throws IOException {
    try {
      requestRestore(new ExtendedBlock(poolId, blockId, length, generationStamp));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
