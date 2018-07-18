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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
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
import backup.namenode.report.BackupReportWriter;
import backup.namenode.report.BackupReportWriterToFileSystem;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.store.ExternalExtendedBlockSort;

public class NameNodeBackupBlockCheckProcessor extends BaseProcessor {

  private static final String SNAPSHOT = ".snapshot";

  private static final String DFS_NAMENODE_NAME_DIR = "dfs.namenode.name.dir";

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
  private final File _reportPath;

  public NameNodeBackupBlockCheckProcessor(Configuration conf, NameNodeRestoreProcessor processor, NameNode namenode,
      UserGroupInformation ugi) throws Exception {
    String[] nnStorageLocations = conf.getStrings(DFS_NAMENODE_NAME_DIR);
    URI uri = new URI(nnStorageLocations[0]);
    _reportPath = new File(new File(uri.getPath()).getParent(), "backup-reports");
    _reportPath.mkdirs();
    if (!_reportPath.exists()) {
      throw new IOException("Report path " + _reportPath + " does not exist");
    }

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

  public File getReportPath() {
    return _reportPath;
  }

  public synchronized void runBlockCheck() throws Exception {
    try (BackupReportWriter writer = getBackupReportWriter()) {
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

  private BackupReportWriter getBackupReportWriter() throws IOException {
    return new BackupReportWriterToFileSystem(_reportPath);
  }

  private void checkBlockPool(String blockPoolId, ExternalExtendedBlockSort<Addresses> nameNodeBlocks,
      ExternalExtendedBlockSort<NullWritable> backupBlocks, BackupReportWriter writer) throws Exception {
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

  private void checkAllBlocks(BackupReportWriter writer, ExtendedBlockEnum<Addresses> nnEnum,
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
            writer.deleteBackupBlockError(bu);
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
            writer.deleteBackupBlockError(bu);
          }
          buEnum.next();
        }
      }
    } finally {
      if (backupBatch.size() > 0) {
        writeBackupRequests(writer, backupBatch);
        backupBatch.clear();
      }
    }
  }

  private void deleteAllFromBackupStore(BackupReportWriter writer, ExtendedBlockEnum<NullWritable> buEnum)
      throws Exception {
    ExtendedBlock block = buEnum.current();
    if (block != null) {
      try {
        writer.deleteBackupBlock(block);
        backupStore.deleteBlock(block);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to delete block " + block, e);
        writer.deleteBackupBlockError(block);
      }
    }
    while ((block = buEnum.next()) != null) {
      try {
        writer.deleteBackupBlock(block);
        backupStore.deleteBlock(block);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to delete block " + block, e);
        writer.deleteBackupBlockError(block);
      }
    }
  }

  private void restoreAll(BackupReportWriter writer, ExtendedBlockEnum<NullWritable> buEnum) throws Exception {
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
          writer.restoreBlockError(block);
        }
      }
    }
    writer.completeRestoreAll();
  }

  private void backupAll(BackupReportWriter writer, ExtendedBlockEnum<Addresses> nnEnum) throws Exception {
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
      writeBackupRequests(writer, batch);
      batch.clear();
    }
    writer.completeBackupAll();
  }

  private void backupBlock(BackupReportWriter writer, List<ExtendedBlockWithAddress> batch,
      ExtendedBlockEnum<Addresses> nnEnum) {
    if (batch.size() >= batchSize) {
      writeBackupRequests(writer, batch);
      batch.clear();
    }
    batch.add(new ExtendedBlockWithAddress(nnEnum.current(), nnEnum.currentValue()));
  }

  private void writeBackupRequests(BackupReportWriter writer, List<ExtendedBlockWithAddress> batch) {
    writer.backupRequestBatch(batch);
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
        writer.backupRequestError(dataNodeAddress, extendedBlockWithAddress);
      }
    }
  }

  private InetSocketAddress chooseOneAtRandom(Addresses address) {
    String[] ipAddrs = address.getIpAddrs();
    int[] ipcPorts = address.getIpcPorts();
    int index = BackupUtil.nextInt(ipAddrs.length);
    return new InetSocketAddress(ipAddrs[index], ipcPorts[index]);
  }

  private ExternalExtendedBlockSort<NullWritable> fetchBlocksFromBackupStore(BackupReportWriter writer)
      throws Exception {
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

  private ExternalExtendedBlockSort<Addresses> fetchBlocksFromNameNode(BackupReportWriter writer) throws Exception {
    writer.startBlockMetaDataFetchFromNameNode();
    Path sortDir = getLocalSort(NAMENODE_SORT);
    ExternalExtendedBlockSort<Addresses> nameNodeBlocks = new ExternalExtendedBlockSort<Addresses>(conf, sortDir,
        Addresses.class);

    Path path = new Path("/");
    // Add normal files
    addExtendedBlocksFromNameNode(writer, nameNodeBlocks, path);
    // Add snapshot dirs
    SnapshottableDirectoryStatus[] snapshottableDirListing = fileSystem.getSnapshottableDirListing();
    for (SnapshottableDirectoryStatus status : snapshottableDirListing) {
      addExtendedBlocksFromNameNode(writer, nameNodeBlocks, new Path(status.getFullPath(), SNAPSHOT));
    }
    writer.completeBlockMetaDataFetchFromNameNode();
    return nameNodeBlocks;
  }

  private void addExtendedBlocksFromNameNode(BackupReportWriter writer,
      ExternalExtendedBlockSort<Addresses> nameNodeBlocks, Path path) throws FileNotFoundException, IOException {
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
    DFSClient client = fileSystem.getClient();
    long st = System.nanoTime();
    while (iterator.hasNext()) {
      FileStatus fs = iterator.next();
      if (st + TimeUnit.SECONDS.toNanos(10) < System.nanoTime()) {
        writer.statusBlockMetaDataFetchFromNameNode(fs.getPath()
                                                      .toString());
        st = System.nanoTime();
      }
      addExtendedBlocksFromNameNode(writer, nameNodeBlocks, client, fs);
    }
  }

  private void addExtendedBlocksFromNameNode(BackupReportWriter writer,
      ExternalExtendedBlockSort<Addresses> nameNodeBlocks, DFSClient client, FileStatus fs) throws IOException {
    String src = fs.getPath()
                   .toUri()
                   .getPath();
    long start = 0;
    long length = fs.getLen();

    LocatedBlocks locatedBlocks = client.getLocatedBlocks(src, start, length);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      DatanodeInfo[] locations = locatedBlock.getLocations();
      ExtendedBlock extendedBlock = BackupUtil.fromHadoop(locatedBlock.getBlock());
      Addresses addresses = new Addresses(locations);
      nameNodeBlocks.add(extendedBlock, addresses);
      writer.statusExtendedBlocksFromNameNode(src, extendedBlock, locations);
    }
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

    @Override
    public String toString() {
      return "Addresses [ipAddrs=" + Arrays.toString(ipAddrs) + ", ipcPorts=" + Arrays.toString(ipcPorts) + "]";
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

    @Override
    public String toString() {
      return "ExtendedBlockWithAddress [extendedBlock=" + extendedBlock + ", addresses=" + addresses + "]";
    }

  }

  public void runReport() {
    Thread thread = new Thread(() -> {
      try {
        runBlockCheck();
      } catch (Exception e) {
        LOG.error("Unknown error", e);
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

}
