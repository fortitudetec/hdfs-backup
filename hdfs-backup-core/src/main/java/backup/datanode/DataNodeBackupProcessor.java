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

import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_RETRY_DELAY_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RETRY_DELAY_DEFAULT;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.namenode.ipc.DatanodeUuids;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.LengthInputStream;

public class DataNodeBackupProcessor extends DataNodeBackupProcessorBase {

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeBackupProcessor.class);

  private final DataNode _datanode;
  private final BackupStore _backupStore;
  private final NameNodeClient _nameNodeClient;
  private final long _retryDelay;

  public DataNodeBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    super(conf);
    _retryDelay = conf.getLong(DFS_BACKUP_DATANODE_BACKUP_RETRY_DELAY_KEY, DFS_BACKUP_DATANODE_RETRY_DELAY_DEFAULT);
    _backupStore = _closer.register(BackupStore.create(BackupUtil.convert(conf)));
    _datanode = datanode;
    _nameNodeClient = new NameNodeClient(conf, UserGroupInformation.getCurrentUser());
  }

  @Override
  protected long doBackup(ExtendedBlock extendedBlock, boolean force) throws Exception {
    if (!force) {
      long blockId = extendedBlock.getBlockId();
      DatanodeUuids result;
      try {
        result = _nameNodeClient.getDatanodeUuids(blockId);
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.error("datanode UUID lookup failed " + e.getCause(), e);
        } else {
          LOG.error("datanode UUID lookup failed {}", e.getCause());
        }
        result = new DatanodeUuids();
      }
      List<String> datanodeUuids = result.getDatanodeUuids();
      if (!datanodeUuids.isEmpty()) {
        LOG.debug("datanode UUIDs for block id {} {}", blockId, datanodeUuids);
        List<String> orderedDataNodeUuids = getOrderDataNodeUuids(datanodeUuids, blockId);
        String datanodeUuid = _datanode.getDatanodeUuid();
        int binarySearch = Collections.binarySearch(orderedDataNodeUuids, datanodeUuid);
        if (binarySearch == 0) {
          LOG.debug("datanode UUID {} first in list {} for block id {}, performing backup", datanodeUuid, datanodeUuids,
              blockId);
        } else if (binarySearch > 0) {
          return binarySearch * _retryDelay;
        } else {
          LOG.debug("datanode UUID {} not found in list {} for block id {}, forcing backup", datanodeUuid,
              datanodeUuids, blockId);
        }
      } else {
        LOG.debug("datanode UUIDs for block id {} empty, forcing backup", blockId);
      }
    }

    if (_backupStore.hasBlock(extendedBlock)) {
      LOG.debug("block {} already backed up", extendedBlock);
      return 0;
    }
    FsDatasetSpi<?> fsDataset = _datanode.getFSDataset();
    org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = BackupUtil.toHadoop(extendedBlock);
    LOG.info("performing block {}", extendedBlock);
    long numBytes = heb.getNumBytes();
    try (LengthInputStream data = new LengthInputStream(trackThroughPut(fsDataset.getBlockInputStream(heb, 0)),
        numBytes)) {
      org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream tmeta;
      tmeta = fsDataset.getMetaDataInputStream(heb);
      try (LengthInputStream meta = new LengthInputStream(trackThroughPut(tmeta), tmeta.getLength())) {
        _backupStore.backupBlock(extendedBlock, data, meta);
        return 0;
      }
    } catch (IOException e) {
      if (LOG.isDebugEnabled() || !e.getMessage()
                                    .contains("is not valid")) {
        LOG.error(e.getMessage(), e);
      } else {
        LOG.debug("block {} has been removed {}", extendedBlock);
      }
      return 0;
    }
  }

  private List<String> getOrderDataNodeUuids(List<String> datanodeUuids, long blockId) {
    List<String> result = new ArrayList<>(datanodeUuids);
    Collections.shuffle(result, new Random(blockId));
    return result;
  }

  private InputStream trackThroughPut(InputStream input) {
    return new ThroughPutInputStream(input, _backupThroughput);
  }

}