package backup.namenode;

import static backup.BackupConstants.BACKUP_BLOCK_REQUESTS;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.BackupExtendedBlocks;
import backup.BaseProcessor;
import backup.store.BackupStore;
import backup.store.ExtendedBlockEnum;
import backup.store.ExternalExtendedBlockSort;
import backup.zookeeper.ZkUtils;
import backup.zookeeper.ZooKeeperClient;

public class NameNodeBackupBlockCheckProcessor extends BaseProcessor {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupBlockCheckProcessor.class);

  private static final String NAMENODE_SORT = "namenode-sort/";
  private static final String BACKUPSTORE_SORT = "backupstore-sort/";

  private final NameNodeBackupProcessor processor;
  private final long checkInterval;
  private final int initInterval;
  private final DistributedFileSystem fileSystem;
  private final Configuration conf;
  private final BackupStore backupStore;
  private final ZooKeeperClient zooKeeper;

  private int batchSize;

  public NameNodeBackupBlockCheckProcessor(Configuration conf, ZooKeeperClient zooKeeper,
      NameNodeBackupProcessor processor) throws Exception {
    this.conf = conf;
    this.zooKeeper = zooKeeper;
    this.processor = processor;
    backupStore = BackupStore.create(conf);
    this.fileSystem = (DistributedFileSystem) FileSystem.get(conf);
    this.checkInterval = conf.getLong(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT);
    this.initInterval = conf.getInt(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT);
    start();
  }

  public void runBlockCheck() throws Exception {
    ExternalExtendedBlockSort nameNodeBlocks = fetchBlocksFromNameNode();
    ExternalExtendedBlockSort backupBlocks = fetchBlocksFromBackupStore();
    try {
      nameNodeBlocks.finished();
      backupBlocks.finished();

      Set<String> blockPoolIds = new HashSet<>();
      blockPoolIds.addAll(nameNodeBlocks.getBlockPoolIds());
      blockPoolIds.addAll(backupBlocks.getBlockPoolIds());

      for (String blockPoolId : blockPoolIds) {
        checkBlockPool(blockPoolId, nameNodeBlocks, backupBlocks);
      }
    } finally {
      IOUtils.closeQuietly(nameNodeBlocks);
      IOUtils.closeQuietly(backupBlocks);
    }
  }

  private void checkBlockPool(String blockPoolId, ExternalExtendedBlockSort nameNodeBlocks,
      ExternalExtendedBlockSort backupBlocks) throws Exception {
    ExtendedBlockEnum nnEnum = null;
    ExtendedBlockEnum buEnum = null;
    try {
      nnEnum = nameNodeBlocks.getBlockEnum(blockPoolId);
      buEnum = backupBlocks.getBlockEnum(blockPoolId);

      if (nnEnum == null) {
        restoreAll(buEnum);
      } else if (buEnum == null) {
        backupAll(nnEnum);
      } else {
        checkAllBlocks(nnEnum, buEnum);
      }
    } finally {
      IOUtils.closeQuietly(buEnum);
      IOUtils.closeQuietly(nnEnum);
    }
  }

  private void checkAllBlocks(ExtendedBlockEnum nnEnum, ExtendedBlockEnum buEnum) throws Exception {
    nnEnum.next();
    buEnum.next();
    List<ExtendedBlock> backupBatch = new ArrayList<>();
    try {
      while (true) {
        if (buEnum.current() == null && nnEnum.current() == null) {
          return;
        } else if (buEnum.current() == null) {
          backupAll(nnEnum, nnEnum.current());
          return;
        } else if (nnEnum.current() == null) {
          deleteAllFromBackupStore(buEnum);
          return;
        }

        ExtendedBlock nn = nnEnum.current();
        ExtendedBlock bu = buEnum.current();

        if (nn.equals(bu)) {
          nnEnum.next();
          buEnum.next();
          // nothing to do
          continue;
        }

        int compare = Long.compare(nn.getBlockId(), bu.getBlockId());
        if (compare == 0) {
          // Blocks not equal but block ids present in both reports. Backup.
          backupStore.deleteBlock(bu);
          backupBlock(backupBatch, nn);
          nnEnum.next();
          buEnum.next();
        } else if (compare < 0) {
          // nn 123, bu 124
          // Missing backup block. Backup.
          backupBlock(backupBatch, nn);
          nnEnum.next();
        } else {
          // nn 125, bu 124
          // Missing namenode block. Remove from backup.
          backupStore.deleteBlock(bu);
          buEnum.next();
        }
      }
    } finally {
      if (backupBatch.size() > 0) {
        writeBackupRequests(backupBatch);
        backupBatch.clear();
      }
    }
  }

  private void deleteAllFromBackupStore(ExtendedBlockEnum buEnum) throws Exception {
    if (buEnum.current() != null) {
      backupStore.deleteBlock(buEnum.current());
    }
    ExtendedBlock block;
    while ((block = buEnum.next()) != null) {
      backupStore.deleteBlock(block);
    }
  }

  private void restoreAll(ExtendedBlockEnum buEnum) throws Exception {
    ExtendedBlock block;
    while ((block = buEnum.next()) != null) {
      processor.requestRestore(block);
    }
  }

  private void backupAll(ExtendedBlockEnum nnEnum) throws Exception {
    backupAll(nnEnum, null);
  }

  private void backupAll(ExtendedBlockEnum nnEnum, ExtendedBlock current) throws Exception {
    List<ExtendedBlock> batch = new ArrayList<>();
    if (current != null) {
      batch.add(current);
    }
    ExtendedBlock block;
    while ((block = nnEnum.next()) != null) {
      backupBlock(batch, block);
    }
    if (batch.size() > 0) {
      writeBackupRequests(batch);
      batch.clear();
    }
  }

  private void backupBlock(List<ExtendedBlock> batch, ExtendedBlock block) throws Exception {
    if (batch.size() >= batchSize) {
      writeBackupRequests(batch);
      batch.clear();
    }
    batch.add(block);
  }

  private void writeBackupRequests(List<ExtendedBlock> batch) throws Exception {
    batch.forEach(block -> LOG.info("Backup block request {}", block));
    String path = ZkUtils.createPath(BACKUP_BLOCK_REQUESTS, "batch-");
    byte[] data = BackupExtendedBlocks.toBytes(batch);
    zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  private ExternalExtendedBlockSort fetchBlocksFromBackupStore() throws Exception {
    Path sortDir = getLocalSort(BACKUPSTORE_SORT);
    ExternalExtendedBlockSort backupBlocks = new ExternalExtendedBlockSort(conf, sortDir);
    ExtendedBlockEnum extendedBlocks = backupStore.getExtendedBlocks();
    ExtendedBlock block;
    while ((block = extendedBlocks.next()) != null) {
      backupBlocks.add(block);
    }
    return backupBlocks;
  }

  private ExternalExtendedBlockSort fetchBlocksFromNameNode() throws Exception {
    Path sortDir = getLocalSort(NAMENODE_SORT);
    ExternalExtendedBlockSort nameNodeBlocks = new ExternalExtendedBlockSort(conf, sortDir);
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
    DFSClient client = fileSystem.getClient();
    while (iterator.hasNext()) {
      FileStatus fs = iterator.next();
      String src = fs.getPath()
                     .toUri()
                     .getPath();
      long start = 0;
      long length = fs.getLen();
      LocatedBlocks locatedBlocks = client.getLocatedBlocks(src, start, length);
      for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
        nameNodeBlocks.add(locatedBlock.getBlock());
      }
    }
    return nameNodeBlocks;
  }

  private Path getLocalSort(String name) throws IOException {
    Path sortDir = conf.getLocalPath(DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY, name);
    LocalFileSystem local = FileSystem.getLocal(conf);
    sortDir = sortDir.makeQualified(local.getUri(), local.getWorkingDirectory());
    return sortDir;
  }

  @Override
  protected void initInternal() throws Exception {
    Thread.sleep(new Random().nextInt(initInterval));
  }

  @Override
  protected void closeInternal() {

  }

  @Override
  protected void runInternal() throws Exception {
    try {
      runBlockCheck();
    } catch (Throwable t) {
      LOG.error("Unknown error during block check", t);
      throw t;
    } finally {
      Thread.sleep(checkInterval);
    }
  }

}
