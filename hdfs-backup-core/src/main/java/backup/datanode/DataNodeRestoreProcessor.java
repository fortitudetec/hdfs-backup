package backup.datanode;

import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import backup.Executable;
import backup.store.BackupStore;
import backup.store.ConfigurationConverter;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockConverter;
import backup.store.WritableExtendedBlock;

public class DataNodeRestoreProcessor implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeRestoreProcessor.class);

  private final DataNode datanode;
  private final BackupStore backupStore;
  private final int bytesPerChecksum;
  private final Type checksumType;
  private final BlockingQueue<ExtendedBlock> restoreBlocks = new LinkedBlockingQueue<>();
  private final ExecutorService executorService;
  private final long pauseOnError;
  private final Closer closer;
  private final AtomicBoolean running = new AtomicBoolean(true);

  public DataNodeRestoreProcessor(Configuration conf, DataNode datanode) throws Exception {
    this.closer = Closer.create();
    this.datanode = datanode;
    this.bytesPerChecksum = conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    this.checksumType = Type
        .valueOf(conf.get(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT));

    int threads = conf.getInt(DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_KEY,
        DFS_BACKUP_DATANODE_RESTORE_BLOCK_HANDLER_COUNT_DEFAULT);

    this.pauseOnError = conf.getLong(DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_KEY,
        DFS_BACKUP_DATANODE_RESTORE_ERROR_PAUSE_DEFAULT);

    backupStore = BackupStore.create(ConfigurationConverter.convert(conf));
    executorService = Executors.newFixedThreadPool(threads);
    closer.register((Closeable) () -> executorService.shutdownNow());
    for (int t = 0; t < threads; t++) {
      Runnable runnable = Executable.createDaemon(LOG, pauseOnError, running, () -> restoreBlocks());
      executorService.submit(runnable);
    }
  }

  @Override
  public void close() throws IOException {
    running.set(false);
    IOUtils.closeQuietly(closer);
  }

  public void addToRestoreQueue(WritableExtendedBlock extendedBlock) throws IOException {
    try {
      restoreBlocks.put(extendedBlock.getExtendedBlock());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean restoreBlocks() throws Exception {
    ExtendedBlock extendedBlock = restoreBlocks.take();
    try {
      restoreBlock(extendedBlock);
    } catch (Exception e) {
      LOG.error("Unknown error", e);
      // try again
      restoreBlocks.put(extendedBlock);
    }
    return true;
  }

  public void restoreBlock(ExtendedBlock extendedBlock) throws Exception {
    if (!backupStore.hasBlock(extendedBlock)) {
      LOG.error("Can not restore block, not in block store {}", extendedBlock);
      return;
    }
    FsDatasetSpi<?> fsDataset = datanode.getFSDataset();
    org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = ExtendedBlockConverter.toHadoop(extendedBlock);
    if (fsDataset.isValidBlock(heb)) {
      LOG.info("Block already restored {}", extendedBlock);
      return;
    }
    LOG.info("Restoring block {}", extendedBlock);
    boolean allowLazyPersist = true;
    // org.apache.hadoop.fs.StorageType storageType =
    // org.apache.hadoop.fs.StorageType.DEFAULT;
    org.apache.hadoop.hdfs.StorageType storageType = org.apache.hadoop.hdfs.StorageType.DEFAULT;
    ReplicaHandler replicaHandler = fsDataset.createRbw(storageType, heb, allowLazyPersist);
    ReplicaInPipelineInterface pipelineInterface = replicaHandler.getReplica();
    boolean isCreate = true;
    DataChecksum requestedChecksum = DataChecksum.newDataChecksum(checksumType, bytesPerChecksum);
    int bytesCopied = 0;
    try (ReplicaOutputStreams streams = pipelineInterface.createStreams(isCreate, requestedChecksum)) {
      try (OutputStream checksumOut = streams.getChecksumOut()) {
        try (InputStream metaData = backupStore.getMetaDataInputStream(extendedBlock)) {
          LOG.info("Restoring meta data for block {}", extendedBlock);
          IOUtils.copy(metaData, checksumOut);
        }
      }
      try (OutputStream dataOut = streams.getDataOut()) {
        try (InputStream data = backupStore.getDataInputStream(extendedBlock)) {
          LOG.info("Restoring data for block {}", extendedBlock);
          bytesCopied = IOUtils.copy(data, dataOut);
        }
      }
    }
    pipelineInterface.setNumBytes(bytesCopied);
    LOG.info("Finalizing restored block {}", extendedBlock);
    fsDataset.finalizeBlock(heb);

    // datanode.notifyNamenodeReceivedBlock(extendedBlock, "",
    // pipelineInterface.getStorageUuid();
    datanode.notifyNamenodeReceivedBlock(heb, "", pipelineInterface.getStorageUuid(),
        pipelineInterface.isOnTransientStorage());
  }

}
