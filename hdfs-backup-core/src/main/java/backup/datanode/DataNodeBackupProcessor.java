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
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_VALIDATE_DELAY_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_VALIDATE_DELAY_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_VALIDATE_PERIOD_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_VALIDATE_PERIOD_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_VALIDATE_RETRY_COUNT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_VALIDATE_RETRY_COUNT_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RETRY_DELAY_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
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

  private static final String RW = "rw";
  private static final String VALID = ".valid";
  private static final String BLOCK_CHECK = "block-check";

  private final DataNode _datanode;
  private final BackupStore _backupStore;
  private final NameNodeClient _nameNodeClient;
  private final long _retryDelay;
  private final ExecutorService _validatePool;
  private final int _validateRetries;
  private final AtomicBoolean _validateRunning;
  private final Timer _timer;

  public DataNodeBackupProcessor(Configuration conf, DataNode datanode) throws Exception {
    super(conf);
    _retryDelay = conf.getLong(DFS_BACKUP_DATANODE_BACKUP_RETRY_DELAY_KEY, DFS_BACKUP_DATANODE_RETRY_DELAY_DEFAULT);
    _validateRetries = conf.getInt(DFS_BACKUP_DATANODE_BACKUP_VALIDATE_RETRY_COUNT_KEY,
        DFS_BACKUP_DATANODE_BACKUP_VALIDATE_RETRY_COUNT_DEFAULT);
    _backupStore = _closer.register(BackupStore.create(BackupUtil.convert(conf)));
    _datanode = datanode;
    _nameNodeClient = new NameNodeClient(conf, UserGroupInformation.getCurrentUser());
    _validatePool = _closer.register(Executors.newSingleThreadExecutor());
    _validateRunning = new AtomicBoolean();
    _timer = new Timer(BLOCK_CHECK, true);
    long delay = TimeUnit.MINUTES.toMillis(
        conf.getInt(DFS_BACKUP_DATANODE_BACKUP_VALIDATE_DELAY_KEY, DFS_BACKUP_DATANODE_BACKUP_VALIDATE_DELAY_DEFAULT));
    long period = TimeUnit.MINUTES.toMillis(conf.getInt(DFS_BACKUP_DATANODE_BACKUP_VALIDATE_PERIOD_KEY,
        DFS_BACKUP_DATANODE_BACKUP_VALIDATE_PERIOD_DEFAULT));
    _timer.schedule(getBlockCheckTimer(), delay, period);
  }

  private TimerTask getBlockCheckTimer() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          FsDatasetSpi<?> fsDataset = _datanode.getFSDataset();
          FsVolumeReferences fsVolumeReferences = fsDataset.getFsVolumeReferences();
          Set<String> bpidSet = new HashSet<>();
          for (FsVolumeSpi volumeSpi : fsVolumeReferences) {
            String[] blockPoolList = volumeSpi.getBlockPoolList();
            for (String bpid : blockPoolList) {
              bpidSet.add(bpid);
            }
          }
          for (String bpid : bpidSet) {
            try {
              runBlockCheck(true, false, bpid);
            } catch (Exception e) {
              LOG.error("Unknown error during block check of " + bpid, e);
            }
          }
        } catch (Throwable t) {
          LOG.error("Unknown error", t);
        }
      }
    };
  }

  @Override
  public void close() {
    _timer.purge();
    _timer.cancel();
    super.close();
  }

  public synchronized void runBlockCheck(boolean blocking, boolean ignorePreviousChecks, String bpid) throws Exception {
    if (_validateRunning.get()) {
      LOG.info("Validation already running.");
      return;
    }
    _validateRunning.set(true);
    Callable<Void> callable = () -> {
      long start = System.nanoTime();
      try {
        LOG.info("Running block check on bpid {}", bpid);
        FsDatasetSpi<?> fsDataset = _datanode.getFSDataset();
        List<FinalizedReplica> list = fsDataset.getFinalizedBlocksOnPersistentStorage(bpid);
        for (FinalizedReplica block : list) {
          validateBlockIsBackedUp(ignorePreviousChecks, bpid, block, fsDataset);
        }
      } finally {
        long end = System.nanoTime();
        LOG.info("Finished block check on bpid {} took {} seconds", bpid, (end - start) / 1_000_000_000L);
        _validateRunning.set(false);
      }
      return null;
    };
    if (blocking) {
      callable.call();
    } else {
      _validatePool.submit(callable);
    }
  }

  private void validateBlockIsBackedUp(boolean ignorePreviousChecks, String bpid, Block block,
      FsDatasetSpi<?> fsDataset) {
    ExtendedBlock extendedBlock = new ExtendedBlock(bpid, block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp());
    for (int i = 0; i < _validateRetries; i++) {
      try {
        if (ignorePreviousChecks || !alreadyValidated(fsDataset, extendedBlock)) {
          doBackup(extendedBlock, true);
          addToValidationCache(fsDataset, extendedBlock);
        }
        return;
      } catch (Exception e) {
        LOG.error("Unknown error while trying to validate block, backoff " + extendedBlock, e);
        try {
          Thread.sleep(_retryDelay);
        } catch (InterruptedException ex) {
          return;
        }
      }
    }
  }

  private void addToValidationCache(FsDatasetSpi<?> fsDataset, ExtendedBlock extendedBlock) throws IOException {
    File validateFile = getValidateCache(fsDataset, extendedBlock);
    new RandomAccessFile(validateFile, RW).close();
  }

  private File getValidateCache(FsDatasetSpi<?> fsDataset, ExtendedBlock extendedBlock) throws IOException {
    org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = BackupUtil.toHadoop(extendedBlock);
    BlockLocalPathInfo pathInfo = fsDataset.getBlockLocalPathInfo(heb);
    String metaPath = pathInfo.getMetaPath();
    File validateFile = new File(metaPath + VALID);
    return validateFile;
  }

  private boolean alreadyValidated(FsDatasetSpi<?> fsDataset, ExtendedBlock extendedBlock) throws IOException {
    File validateCache = getValidateCache(fsDataset, extendedBlock);
    return validateCache.exists();
  }

  @Override
  protected long doBackup(ExtendedBlock extendedBlock, boolean force) throws Exception {
    if (!force) {
      long blockId = extendedBlock.getBlockId();
      DatanodeUuids result;
      try {
        Block block = new Block(extendedBlock.getBlockId(), extendedBlock.getLength(),
            extendedBlock.getGenerationStamp());
        result = _nameNodeClient.getDatanodeUuids(extendedBlock.getPoolId(), block);
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
    if (!fsDataset.isValidBlock(heb)) {
      return 0;
    }
    BlockLocalPathInfo info = fsDataset.getBlockLocalPathInfo(heb);
    String blockPath = info.getBlockPath();
    if (Files.isSymbolicLink(new File(blockPath).toPath())) {
      LOG.debug("block {} is symbolic link, not backing up", extendedBlock);
      return 0;
    }

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