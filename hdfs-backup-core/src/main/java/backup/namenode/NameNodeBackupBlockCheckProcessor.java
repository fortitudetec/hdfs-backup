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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backup.BaseProcessor;
import backup.datanode.DataNodeBackupRPC;
import backup.store.BackupStore;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.store.ExternalExtendedBlockSort;
import backup.store.WritableExtendedBlock;

public class NameNodeBackupBlockCheckProcessor extends BaseProcessor {

  private final static Logger LOG = LoggerFactory.getLogger(NameNodeBackupBlockCheckProcessor.class);

  private static final String NAMENODE_SORT = "namenode-sort/";
  private static final String BACKUPSTORE_SORT = "backupstore-sort/";

  private final NameNodeRestoreProcessor processor;
  private final long checkInterval;
  private final int initInterval;
  private final DistributedFileSystem fileSystem;
  private final Configuration conf;
  private final BackupStore backupStore;
  private final int batchSize;

  public NameNodeBackupBlockCheckProcessor(Configuration conf, NameNodeRestoreProcessor processor) throws Exception {
    this.conf = conf;
    this.processor = processor;
    backupStore = BackupStore.create(BackupUtil.convert(conf));
    this.fileSystem = (DistributedFileSystem) FileSystem.get(conf);
    this.batchSize = conf.getInt(DFS_BACKUP_REMOTE_BACKUP_BATCH_KEY, DFS_BACKUP_REMOTE_BACKUP_BATCH_DEFAULT);
    this.checkInterval = conf.getLong(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DEFAULT);
    this.initInterval = conf.getInt(DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_KEY,
        DFS_BACKUP_NAMENODE_BLOCK_CHECK_INTERVAL_DELAY_DEFAULT);
    start();
  }

  public void runBlockCheck() throws Exception {
    ExternalExtendedBlockSort<Addresses> nameNodeBlocks = fetchBlocksFromNameNode();
    ExternalExtendedBlockSort<NullWritable> backupBlocks = fetchBlocksFromBackupStore();
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

  private void checkBlockPool(String blockPoolId, ExternalExtendedBlockSort<Addresses> nameNodeBlocks,
      ExternalExtendedBlockSort<NullWritable> backupBlocks) throws Exception {
    ExtendedBlockEnum<Addresses> nnEnum = null;
    ExtendedBlockEnum<NullWritable> buEnum = null;
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

  private void checkAllBlocks(ExtendedBlockEnum<Addresses> nnEnum, ExtendedBlockEnum<NullWritable> buEnum)
      throws Exception {
    nnEnum.next();
    buEnum.next();
    List<ExtendedBlockWithAddress> backupBatch = new ArrayList<>();
    try {
      while (true) {
        if (buEnum.current() == null && nnEnum.current() == null) {
          return;
        } else if (buEnum.current() == null) {
          backupAll(nnEnum);
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
          backupBlock(backupBatch, nnEnum);
          nnEnum.next();
          buEnum.next();
        } else if (compare < 0) {
          // nn 123, bu 124
          // Missing backup block. Backup.
          backupBlock(backupBatch, nnEnum);
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

  private void deleteAllFromBackupStore(ExtendedBlockEnum<NullWritable> buEnum) throws Exception {
    if (buEnum.current() != null) {
      backupStore.deleteBlock(buEnum.current());
    }
    ExtendedBlock block;
    while ((block = buEnum.next()) != null) {
      backupStore.deleteBlock(block);
    }
  }

  private void restoreAll(ExtendedBlockEnum<NullWritable> buEnum) throws Exception {
    ExtendedBlock block;
    while ((block = buEnum.next()) != null) {
      processor.requestRestore(block);
    }
  }

  private void backupAll(ExtendedBlockEnum<Addresses> nnEnum) throws Exception {
    List<ExtendedBlockWithAddress> batch = new ArrayList<>();
    if (nnEnum.current() != null) {
      backupBlock(batch, nnEnum);
    }
    while (nnEnum.next() != null) {
      backupBlock(batch, nnEnum);
    }
    if (batch.size() > 0) {
      writeBackupRequests(batch);
      batch.clear();
    }
  }

  private void backupBlock(List<ExtendedBlockWithAddress> batch, ExtendedBlockEnum<Addresses> nnEnum) throws Exception {
    if (batch.size() >= batchSize) {
      writeBackupRequests(batch);
      batch.clear();
    }
    batch.add(new ExtendedBlockWithAddress(nnEnum.current(), nnEnum.currentValue()));
  }

  private void writeBackupRequests(List<ExtendedBlockWithAddress> batch) throws Exception {
    for (ExtendedBlockWithAddress extendedBlockWithAddress : batch) {
      LOG.info("Backup block request {}", extendedBlockWithAddress.getExtendedBlock());
      DataNodeBackupRPC backup = RPC.getProxy(DataNodeBackupRPC.class, RPC.getProtocolVersion(DataNodeBackupRPC.class),
          chooseOneAtRandom(extendedBlockWithAddress.getAddresses()), conf);
      backup.backupBlock(new WritableExtendedBlock(extendedBlockWithAddress.getExtendedBlock()));
    }

  }

  private InetSocketAddress chooseOneAtRandom(Addresses address) {
    String[] ipAddrs = address.getIpAddrs();
    int[] ipcPorts = address.getIpcPorts();
    int index = BackupUtil.nextInt(ipAddrs.length);
    return new InetSocketAddress(ipAddrs[index], ipcPorts[index]);
  }

  private ExternalExtendedBlockSort<NullWritable> fetchBlocksFromBackupStore() throws Exception {
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

  private ExternalExtendedBlockSort<Addresses> fetchBlocksFromNameNode() throws Exception {
    Path sortDir = getLocalSort(NAMENODE_SORT);
    ExternalExtendedBlockSort<Addresses> nameNodeBlocks = new ExternalExtendedBlockSort<Addresses>(conf, sortDir,
        Addresses.class);
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
    DFSClient client = fileSystem.getClient();
    while (iterator.hasNext()) {
      FileStatus fs = iterator.next();
      String src = fs.getPath().toUri().getPath();
      long start = 0;
      long length = fs.getLen();
      LocatedBlocks locatedBlocks = client.getLocatedBlocks(src, start, length);
      for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
        DatanodeInfo[] locations = locatedBlock.getLocations();
        ExtendedBlock extendedBlock = BackupUtil.fromHadoop(locatedBlock.getBlock());
        nameNodeBlocks.add(extendedBlock, new Addresses(locations));
      }
    }
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

  public static class Addresses implements Writable {

    private String[] ipAddrs;
    private int[] ipcPorts;

    public Addresses() {

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
