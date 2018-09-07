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

import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import backup.Executable;
import backup.datanode.ipc.RestoreStats;
import backup.metrics.Metrics;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.util.Closer;

public class DataNodeRestoreProcessor implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeRestoreProcessor.class);

  private static final String RESTORE_THROUGHPUT = "restoreThroughput";
  private final DataNode _datanode;
  private final BackupStore _backupStore;
  private final int _bytesPerChecksum;
  private final Type _checksumType;
  private final BlockingQueue<ExtendedBlock> _restoreBlocks;
  private final ExecutorService _executorService;
  private final Closer _closer;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final AtomicInteger _restoreInProgress = new AtomicInteger();
  private final Meter _restoreThroughput;

  public DataNodeRestoreProcessor(Configuration conf, DataNode datanode) throws Exception {
    _closer = Closer.create();
    _datanode = datanode;
    _restoreThroughput = Metrics.METRICS.meter(RESTORE_THROUGHPUT);
    _bytesPerChecksum = conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    _checksumType = Type.valueOf(
        conf.get(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT));
    int threads = conf.getInt(DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_KEY,
        DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_DEFAULT);
    long pauseOnError = conf.getLong(DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_KEY,
        DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_DEFAULT);
    _backupStore = _closer.register(BackupStore.create(BackupUtil.convert(conf)));
    _restoreBlocks = new ArrayBlockingQueue<>(threads);
    _executorService = Executors.newCachedThreadPool();
    _closer.register((Closeable) () -> _executorService.shutdownNow());
    for (int t = 0; t < threads; t++) {
      _executorService.submit(Executable.createDaemon(LOG, pauseOnError, _running, () -> restoreBlocks()));
    }
  }

  public RestoreStats getRestoreStats() {
    RestoreStats restoreStats = new RestoreStats();
    restoreStats.setRestoreBlocks(_restoreBlocks.size());
    restoreStats.setRestoresInProgressCount(_restoreInProgress.get());
    restoreStats.setRestoreBytesPerSecond(_restoreThroughput.getOneMinuteRate());
    return restoreStats;
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    IOUtils.closeQuietly(_closer);
  }

  public boolean addToRestoreQueue(ExtendedBlock extendedBlock) throws IOException {
    try {
      return _restoreBlocks.offer(extendedBlock);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean restoreBlocks() throws Exception {
    ExtendedBlock extendedBlock = _restoreBlocks.take();
    try {
      restoreBlock(extendedBlock);
    } catch (Exception e) {
      LOG.error("Unknown error", e);
      // try again
      _restoreBlocks.put(extendedBlock);
    }
    return true;
  }

  public void restoreBlock(ExtendedBlock extendedBlock) throws Exception {
    if (!_backupStore.hasBlock(extendedBlock)) {
      LOG.error("Can not restore block, not in block store {}", extendedBlock);
      return;
    }
    FsDatasetSpi<?> fsDataset = _datanode.getFSDataset();
    org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = BackupUtil.toHadoop(extendedBlock);
    if (fsDataset.isValidBlock(heb)) {
      LOG.info("Block already restored {}", extendedBlock);
      return;
    }
    try {
      _restoreInProgress.incrementAndGet();
      LOG.info("Restoring block {}", extendedBlock);
      boolean allowLazyPersist = true;
      // org.apache.hadoop.fs.StorageType storageType =
      // org.apache.hadoop.fs.StorageType.DEFAULT;
      org.apache.hadoop.hdfs.StorageType storageType = org.apache.hadoop.hdfs.StorageType.DEFAULT;
      ReplicaHandler replicaHandler = fsDataset.createRbw(storageType, heb, allowLazyPersist);
      ReplicaInPipelineInterface pipelineInterface = replicaHandler.getReplica();
      boolean isCreate = true;
      DataChecksum requestedChecksum = DataChecksum.newDataChecksum(_checksumType, _bytesPerChecksum);
      int bytesCopied = 0;
      try (ReplicaOutputStreams streams = pipelineInterface.createStreams(isCreate, requestedChecksum)) {
        try (OutputStream checksumOut = streams.getChecksumOut()) {
          try (InputStream metaData = _backupStore.getMetaDataInputStream(extendedBlock)) {
            LOG.info("Restoring meta data for block {}", extendedBlock);
            IOUtils.copy(trackThroughPut(metaData), checksumOut);
          }
        }
        try (OutputStream dataOut = streams.getDataOut()) {
          try (InputStream data = _backupStore.getDataInputStream(extendedBlock)) {
            LOG.info("Restoring data for block {}", extendedBlock);
            bytesCopied = IOUtils.copy(trackThroughPut(data), dataOut);
          }
        }
      }
      pipelineInterface.setNumBytes(bytesCopied);
      LOG.info("Finalizing restored block {}", extendedBlock);
      fsDataset.finalizeBlock(heb);

      // datanode.notifyNamenodeReceivedBlock(extendedBlock, "",
      // pipelineInterface.getStorageUuid();
      _datanode.notifyNamenodeReceivedBlock(heb, "", pipelineInterface.getStorageUuid(),
          pipelineInterface.isOnTransientStorage());
    } catch (ReplicaAlreadyExistsException e) {
      LOG.info("Restoring block already exists {}", extendedBlock);
    } finally {
      _restoreInProgress.decrementAndGet();
    }
  }

  private InputStream trackThroughPut(InputStream input) {
    return new ThroughPutInputStream(input, _restoreThroughput);
  }

  public boolean isRestoringBlock(String poolId, long blockId, long length, long generationStamp) {
    ExtendedBlock block = new ExtendedBlock(poolId, blockId, length, generationStamp);
    return _restoreBlocks.contains(block);
  }
}
