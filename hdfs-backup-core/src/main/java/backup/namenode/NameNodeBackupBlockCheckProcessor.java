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

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY;
import static backup.BackupConstants.DFS_BACKUP_REMOTE_BACKUP_BATCH_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_REMOTE_BACKUP_BATCH_KEY;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.BaseProcessor;
import backup.datanode.ipc.DataNodeBackupRPC;
import backup.namenode.report.BackReportWriter;
import backup.namenode.report.LoggerBackupReportWriter;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.store.ExternalExtendedBlockSort;

public class NameNodeBackupBlockCheckProcessor extends BaseProcessor {

  // private static final String USER_HDFS_BACKUP_REPORTS =
  // "/user/hdfs/.backup/reports";

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupBlockCheckProcessor.class);

  private static final String NAMENODE_SORT = "namenode-sort/";
  private static final String BACKUPSTORE_SORT = "backupstore-sort/";

  private final NameNodeRestoreProcessor processor;
  private final long checkInterval;
  private final long initInterval;
  private final DistributedFileSystem fileSystem;
  private final Configuration conf;
  private final BackupStore backupStore;
  private final int batchSize;
  private final NameNode namenode;
  private final UserGroupInformation ugi;
  // private final Path _reportPath = new Path(USER_HDFS_BACKUP_REPORTS);

  public NameNodeBackupBlockCheckProcessor(Configuration conf, NameNodeRestoreProcessor processor, NameNode namenode,
      UserGroupInformation ugi) throws Exception {
    this.ugi = ugi;
    this.namenode = namenode;
    this.conf = conf;
    this.processor = processor;
    backupStore = BackupStore.create(BackupUtil.convert(conf));
    this.fileSystem = (DistributedFileSystem) FileSystem.get(conf);
    this.batchSize = conf.getInt(DFS_BACKUP_REMOTE_BACKUP_BATCH_KEY, DFS_BACKUP_REMOTE_BACKUP_BATCH_DEFAULT);
    this.checkInterval = conf.getLong(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT);
    this.initInterval = conf.getLong(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT);
    start();
  }

  public void runBlockCheck() throws Exception {
    // new BackupReportWriterToHdfs(fileSystem, _reportPath)
    try (BackReportWriter writer = new LoggerBackupReportWriter()) {
      LOG.info("Starting backup block report.");
      writer.start();
      ExternalExtendedBlockSort<Addresses> nameNodeBlocks = fetchBlocksFromNameNode(writer);
      ExternalExtendedBlockSort<NullWritable> backupBlocks = fetchBlocksFromBackupStore(writer);
      try {
        nameNodeBlocks.finished();
        backupBlocks.finished();

        Set<String> blockPoolIds = new HashSet<>();
        blockPoolIds.addAll(nameNodeBlocks.getBlockPoolIds());
        blockPoolIds.addAll(backupBlocks.getBlockPoolIds());

        for (String blockPoolId : blockPoolIds) {
          writer.startBlockPoolCheck(blockPoolId);
          checkBlockPool(blockPoolId, nameNodeBlocks, backupBlocks, writer);
          writer.completeBlockPoolCheck(blockPoolId);
        }
      } finally {
        IOUtils.closeQuietly(nameNodeBlocks);
        IOUtils.closeQuietly(backupBlocks);
      }
      LOG.info("Finished backup block report.");
      writer.complete();
    }
  }

  private void checkBlockPool(String blockPoolId, ExternalExtendedBlockSort<Addresses> nameNodeBlocks,
      ExternalExtendedBlockSort<NullWritable> backupBlocks, BackReportWriter writer) throws Exception {
    LOG.info("Backup block report for block pool {}.", blockPoolId);
    ExtendedBlockEnum<Addresses> nnEnum = null;
    ExtendedBlockEnum<NullWritable> buEnum = null;
    try {
      nnEnum = nameNodeBlocks.getBlockEnum(blockPoolId);
      buEnum = backupBlocks.getBlockEnum(blockPoolId);
      if (nnEnum == null) {
        restoreAll(writer, buEnum);
      } else if (buEnum == null) {
        backupAll(writer, nnEnum);
      } else {
        checkAllBlocks(writer, nnEnum, buEnum);
      }
    } finally {
      IOUtils.closeQuietly(buEnum);
      IOUtils.closeQuietly(nnEnum);
    }
  }

  private void checkAllBlocks(BackReportWriter writer, ExtendedBlockEnum<Addresses> nnEnum,
      ExtendedBlockEnum<NullWritable> buEnum) throws Exception {
    nnEnum.next();
    buEnum.next();
    List<ExtendedBlockWithAddress> backupBatch = new ArrayList<>();
    try {
      while (true) {
        if (buEnum.current() == null && nnEnum.current() == null) {
          return;
        } else if (buEnum.current() == null) {
          backupAll(writer, nnEnum);
          return;
        } else if (nnEnum.current() == null) {
          deleteAllFromBackupStore(writer, buEnum);
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
          writer.deleteBackupBlock(bu);
          try {
            backupStore.deleteBlock(bu);
          } catch (Exception e) {
            LOG.error("Unknown error while trying to delete block " + bu, e);
          }
          backupBlock(writer, backupBatch, nnEnum);
          nnEnum.next();
          buEnum.next();
        } else if (compare < 0) {
          // nn 123, bu 124
          // Missing backup block. Backup.
          backupBlock(writer, backupBatch, nnEnum);
          nnEnum.next();
        } else {
          // nn 125, bu 124
          // Missing namenode block. Remove from backup.
          try {
            writer.deleteBackupBlock(bu);
            backupStore.deleteBlock(bu);
          } catch (Exception e) {
            LOG.error("Unknown error while trying to delete block " + bu, e);
          }
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

  private void deleteAllFromBackupStore(BackReportWriter writer, ExtendedBlockEnum<NullWritable> buEnum)
      throws Exception {
    ExtendedBlock block = buEnum.current();
    if (block != null) {
      try {
        writer.deleteBackupBlock(block);
        backupStore.deleteBlock(block);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to delete block " + block, e);
      }
    }
    while ((block = buEnum.next()) != null) {
      try {
        writer.deleteBackupBlock(block);
        backupStore.deleteBlock(block);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to delete block " + block, e);
      }
    }
  }

  private void restoreAll(BackReportWriter writer, ExtendedBlockEnum<NullWritable> buEnum) throws Exception {
    writer.startRestoreAll();
    ExtendedBlock block;
    BlockManager blockManager = namenode.getNamesystem()
                                        .getBlockManager();
    while ((block = buEnum.next()) != null) {
      org.apache.hadoop.hdfs.protocol.ExtendedBlock heb = BackupUtil.toHadoop(block);
      BlockCollection blockCollection = blockManager.getBlockCollection(heb.getLocalBlock());
      if (blockCollection == null) {
        try {
          writer.restoreBlock(block);
          processor.requestRestore(block);
        } catch (Exception e) {
          LOG.error("Unknown error while trying to restore block " + block, e);
        }
      }
    }
    writer.completeRestoreAll();
  }

  private void backupAll(BackReportWriter writer, ExtendedBlockEnum<Addresses> nnEnum) throws Exception {
    writer.startBackupAll();
    List<ExtendedBlockWithAddress> batch = new ArrayList<>();
    if (nnEnum.current() != null) {
      backupBlock(writer, batch, nnEnum);
    }
    while (nnEnum.next() != null) {
      backupBlock(writer, batch, nnEnum);
    }
    if (batch.size() > 0) {
      writer.backupRequestBatch(batch);
      writeBackupRequests(batch);
      batch.clear();
    }
    writer.completeBackupAll();
  }

  private void backupBlock(BackReportWriter writer, List<ExtendedBlockWithAddress> batch,
      ExtendedBlockEnum<Addresses> nnEnum) {
    if (batch.size() >= batchSize) {
      writeBackupRequests(batch);
      batch.clear();
    }
    batch.add(new ExtendedBlockWithAddress(nnEnum.current(), nnEnum.currentValue()));
  }

  private void writeBackupRequests(List<ExtendedBlockWithAddress> batch) {
    for (ExtendedBlockWithAddress extendedBlockWithAddress : batch) {
      LOG.info("Backup block request {}", extendedBlockWithAddress.getExtendedBlock());
      Addresses addresses = extendedBlockWithAddress.getAddresses();
      if (addresses == null || addresses.isEmpty()) {
        LOG.info("Block not found on datanodes can not back up {}", extendedBlockWithAddress.getExtendedBlock());
        continue;
      }
      InetSocketAddress dataNodeAddress = chooseOneAtRandom(addresses);
      try {
        DataNodeBackupRPC backup = DataNodeBackupRPC.getDataNodeBackupRPC(dataNodeAddress, conf, ugi);
        ExtendedBlock extendedBlock = extendedBlockWithAddress.getExtendedBlock();
        backup.backupBlock(extendedBlock.getPoolId(), extendedBlock.getBlockId(), extendedBlock.getLength(),
            extendedBlock.getGenerationStamp());
      } catch (IOException | InterruptedException e) {
        LOG.error("Unknown error while trying to request a backup " + extendedBlockWithAddress, e);
      }
    }
  }

  private InetSocketAddress chooseOneAtRandom(Addresses address) {
    String[] ipAddrs = address.getIpAddrs();
    int[] ipcPorts = address.getIpcPorts();
    int index = BackupUtil.nextInt(ipAddrs.length);
    return new InetSocketAddress(ipAddrs[index], ipcPorts[index]);
  }

  private ExternalExtendedBlockSort<NullWritable> fetchBlocksFromBackupStore(BackReportWriter writer) throws Exception {
    Path sortDir = getLocalSort(BACKUPSTORE_SORT);
    ExternalExtendedBlockSort<NullWritable> backupBlocks = new ExternalExtendedBlockSort<NullWritable>(conf, sortDir,
        NullWritable.class);
    ExtendedBlockEnum<Void> extendedBlocks = backupStore.getExtendedBlocks();
    ExtendedBlock block;
    NullWritable value = NullWritable.get();
    while ((block = extendedBlocks.next()) != null) {
      backupBlocks.add(block, value);
    }
    return backupBlocks;
  }

  private ExternalExtendedBlockSort<Addresses> fetchBlocksFromNameNode(BackReportWriter writer) throws Exception {
    writer.startBlockMetaDataFetchFromNameNode();
    Path sortDir = getLocalSort(NAMENODE_SORT);
    ExternalExtendedBlockSort<Addresses> nameNodeBlocks = new ExternalExtendedBlockSort<Addresses>(conf, sortDir,
        Addresses.class);

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
        DatanodeInfo[] locations = locatedBlock.getLocations();
        ExtendedBlock extendedBlock = BackupUtil.fromHadoop(locatedBlock.getBlock());
        nameNodeBlocks.add(extendedBlock, new Addresses(locations));
      }
    }
    writer.completeBlockMetaDataFetchFromNameNode();
    return nameNodeBlocks;
  }

  private Path getLocalSort(String name) throws IOException {
    Path sortDir = conf.getLocalPath(DFS_BACKUP_NAMENODE_LOCAL_DIR_KEY, name);
    LocalFileSystem local = FileSystem.getLocal(conf);
    sortDir = sortDir.makeQualified(local.getUri(), local.getWorkingDirectory());
    local.delete(sortDir, true);
    return sortDir;
  }

  @Override
  protected void initInternal() throws Exception {
    Thread.sleep(initInterval);
  }

  @Override
  protected void closeInternal() {
    IOUtils.closeQuietly(backupStore);
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

  public static class Addresses implements Writable {

    private String[] ipAddrs;
    private int[] ipcPorts;

    public Addresses() {

    }

    public boolean isEmpty() {
      return ipAddrs == null || ipAddrs.length == 0;
    }

    public Addresses(String[] ipAddrs, int[] ipcPorts) {
      this.ipAddrs = ipAddrs;
      this.ipcPorts = ipcPorts;
    }

    public Addresses(DatanodeInfo[] locations) {
      this(BackupUtil.getIpAddrs(locations), BackupUtil.getIpcPorts(locations));
    }

    public String[] getIpAddrs() {
      return ipAddrs;
    }

    public void setIpAddrs(String[] ipAddrs) {
      this.ipAddrs = ipAddrs;
    }

    public int[] getIpcPorts() {
      return ipcPorts;
    }

    public void setIpcPorts(int[] ipcPorts) {
      this.ipcPorts = ipcPorts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      {
        int len = ipAddrs.length;
        out.writeShort(len);
        for (int i = 0; i < len; i++) {
          BackupUtil.writeShortString(ipAddrs[i], out);
        }
      }
      {
        int len = ipcPorts.length;
        out.writeShort(len);
        for (int i = 0; i < len; i++) {
          out.writeInt(ipcPorts[i]);
        }
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      ipAddrs = new String[in.readShort()];
      for (int i = 0; i < ipAddrs.length; i++) {
        ipAddrs[i] = BackupUtil.readShortString(in);
      }
      ipcPorts = new int[in.readShort()];
      for (int i = 0; i < ipcPorts.length; i++) {
        ipcPorts[i] = in.readInt();
      }
    }

  }

  public static class ExtendedBlockWithAddress {
    private final ExtendedBlock extendedBlock;
    private final Addresses addresses;

    public ExtendedBlockWithAddress(ExtendedBlock extendedBlock, Addresses addresses) {
      this.extendedBlock = extendedBlock;
      this.addresses = addresses;
    }

    public ExtendedBlock getExtendedBlock() {
      return extendedBlock;
    }

    public Addresses getAddresses() {
      return addresses;
    }

  }

}
