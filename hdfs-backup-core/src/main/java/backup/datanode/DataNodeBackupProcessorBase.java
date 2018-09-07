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

import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_AGE_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_AGE_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_QUEUE_DEPTH_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_QUEUE_DEPTH_KEY;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_THREAD_COUNT_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_DATANODE_BACKUP_THREAD_COUNT_KEY;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import backup.datanode.ipc.BackupStats;
import backup.metrics.Metrics;
import backup.store.ExtendedBlock;
import backup.util.Closer;

public abstract class DataNodeBackupProcessorBase implements Closeable {

  private static final String BACKUP_THROUGHPUT = "backupThroughput";
  private static final String ENQUEUE_BACKUP_RETRY = "enqueueBackupRetry";
  private static final String ENQUEUE_BACKUP_DROP = "enqueueBackupDrop";
  private static final String QUEUE_BACKUP = "queueBackup";

  private final static Logger LOG = LoggerFactory.getLogger(DataNodeBackupProcessorBase.class);

  protected final Closer _closer;
  protected final AtomicBoolean _running = new AtomicBoolean(true);
  protected final BlockingQueue<ExtendedBlockEntry> _backupQueue;
  protected final ExecutorService _service;
  protected final long _defaultAge;
  protected final Histogram _enqueueBackupDropMetric;
  protected final Histogram _enqueueBackupRetryMetric;
  protected final Counter _backupQueueDepth;
  protected final Meter _backupThroughput;

  public DataNodeBackupProcessorBase(Configuration conf) throws Exception {
    int backupThreads = conf.getInt(DFS_BACKUP_DATANODE_BACKUP_THREAD_COUNT_KEY,
        DFS_BACKUP_DATANODE_BACKUP_THREAD_COUNT_DEFAULT);
    int queueDepth = conf.getInt(DFS_BACKUP_DATANODE_BACKUP_QUEUE_DEPTH_KEY,
        DFS_BACKUP_DATANODE_BACKUP_QUEUE_DEPTH_DEFAULT);
    _defaultAge = conf.getLong(DFS_BACKUP_DATANODE_BACKUP_AGE_KEY, DFS_BACKUP_DATANODE_BACKUP_AGE_DEFAULT);

    _closer = Closer.create();
    _service = _closer.register(Executors.newFixedThreadPool(backupThreads + 1));
    _backupQueue = new PriorityBlockingQueue<>(queueDepth);

    _backupQueueDepth = Metrics.METRICS.counter(QUEUE_BACKUP);
    _enqueueBackupDropMetric = Metrics.METRICS.histogram(ENQUEUE_BACKUP_DROP);
    _enqueueBackupRetryMetric = Metrics.METRICS.histogram(ENQUEUE_BACKUP_RETRY);
    _backupThroughput = Metrics.METRICS.meter(BACKUP_THROUGHPUT);

    startBackupThreads(backupThreads);
  }

  @Override
  public void close() {
    _running.set(false);
    IOUtils.closeQuietly(_closer);
  }

  private void startBackupThreads(int backupThreads) {
    for (int i = 0; i < backupThreads; i++) {
      _service.submit(() -> {
        while (_running.get()) {
          try {
            pullFromQueueAndPerformBackup();
          } catch (Throwable t) {
            if (!_running.get()) {
              return;
            }
            LOG.error("Unknown error during pull queue from blocking queue", t);
            try {
              Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
  }

  public BackupStats getBackupStats() {
    BackupStats backupStats = new BackupStats();
    backupStats.setBackupsInProgressCount(_backupQueue.size());
    backupStats.setBackupBytesPerSecond(_backupThroughput.getOneMinuteRate());
    return backupStats;
  }

  /**
   * This method can not fail or block or this could cause problems for the
   * datanode itself.
   * 
   * @throws Exception
   */
  public void enqueueBackup(ExtendedBlock... extendedBlock) throws Exception {
    offer("Backup", Arrays.asList(extendedBlock), _defaultAge, false);
  }

  private void pullFromQueueAndPerformBackup() throws Exception {
    while (!shouldProcess(_backupQueue.peek())) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    ExtendedBlockEntry extendedBlock = _backupQueue.poll(1, TimeUnit.SECONDS);
    _backupQueueDepth.dec();
    if (extendedBlock == null) {
      return;
    }
    waitUntilOldEnough(extendedBlock);
    Set<ExtendedBlockEntry> allBlocksWithSameBlockId = findAllBlocksWithSameBlockId(extendedBlock.getExtendedBlock());
    ExtendedBlock backUpBlock = selectLongestBlock(allBlocksWithSameBlockId, extendedBlock.getExtendedBlock());
    long result = doBackup(backUpBlock, extendedBlock.isForce());
    if (result != 0) {
      _enqueueBackupRetryMetric.update(1);
      offer("Backup", backUpBlock, result, true);
    }
    for (ExtendedBlockEntry entry : allBlocksWithSameBlockId) {
      if (_backupQueue.remove(entry)) {
        _backupQueueDepth.dec();
      }
    }
  }

  private boolean shouldProcess(ExtendedBlockEntry extendedBlock) {
    if (extendedBlock == null) {
      return false;
    }
    return extendedBlock.shouldProcess();
  }

  private void waitUntilOldEnough(ExtendedBlockEntry extendedBlockEntry) throws InterruptedException {
    long waitTime = extendedBlockEntry.getAttemptBackupTimestamp() - System.currentTimeMillis();
    if (waitTime <= 0) {
      return;
    }
    Thread.sleep(waitTime);
  }

  public void performBackup(ExtendedBlock extendedBlock) throws Exception {
    doBackup(extendedBlock, true);
  }

  private ExtendedBlock selectLongestBlock(Set<ExtendedBlockEntry> blocks, ExtendedBlock extendedBlock) {
    ExtendedBlock result = extendedBlock;
    for (ExtendedBlockEntry entry : blocks) {
      if (entry.getExtendedBlock()
               .getLength() > result.getLength()) {
        result = entry.getExtendedBlock();
      }
    }
    return result;
  }

  private Set<ExtendedBlockEntry> findAllBlocksWithSameBlockId(ExtendedBlock extendedBlock) {
    Set<ExtendedBlockEntry> result = new HashSet<>();
    long blockId = extendedBlock.getBlockId();
    for (ExtendedBlockEntry entry : _backupQueue) {
      if (entry.getExtendedBlock()
               .getBlockId() == blockId) {
        result.add(entry);
      }
    }
    return result;
  }

  protected abstract long doBackup(ExtendedBlock extendedBlock, boolean force) throws Exception;

  private void offer(String name, Collection<ExtendedBlock> blocks, long age, boolean force) {
    for (ExtendedBlock block : blocks) {
      offer(name, block, age, force);
    }
  }

  private void offer(String name, ExtendedBlock block, long age, boolean force) {
    if (_backupQueue.offer(ExtendedBlockEntry.create(block, age, force))) {
      _backupQueueDepth.inc();
    } else {
      LOG.info(name + " queue full, dropping block backup request {}", block);
      _enqueueBackupDropMetric.update(1);
    }
  }

}