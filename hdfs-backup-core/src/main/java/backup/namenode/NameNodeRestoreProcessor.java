package backup.namenode;

import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_DEFAULT;
import static backup.BackupConstants.DFS_BACKUP_NAMENODE_MISSING_BLOCKS_POLL_TIME_KEY;

import java.net.InetSocketAddress;
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
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import backup.BaseProcessor;
import backup.datanode.DataNodeBackupRPC;
import backup.store.BackupUtil;
import backup.store.ExtendedBlock;
import backup.store.WritableExtendedBlock;

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
    Cache<ExtendedBlock, Boolean> cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
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
    Set<DatanodeDescriptor> datanodes = blockManager.getDatanodeManager().getDatanodes();
    DataNodeBackupRPC backup = RPC.getProxy(DataNodeBackupRPC.class, RPC.getProtocolVersion(DataNodeBackupRPC.class),
        getDataNodeAddress(datanodes), conf);
    backup.restoreBlock(new WritableExtendedBlock(extendedBlock));
    currentRequestedRestore.add(extendedBlock);
  }

  private InetSocketAddress getDataNodeAddress(Set<DatanodeDescriptor> storages) {
    DatanodeInfo[] datanodeInfos = storages.toArray(new DatanodeInfo[storages.size()]);
    String[] ipAddrs = BackupUtil.getIpAddrs(datanodeInfos);
    int[] ipcPorts = BackupUtil.getIpcPorts(datanodeInfos);
    int index = BackupUtil.nextInt(ipAddrs.length);
    return new InetSocketAddress(ipAddrs[index], ipcPorts[index]);
  }

  private boolean hasRestoreBeenRequested(ExtendedBlock extendedBlock) {
    return currentRequestedRestore.contains(extendedBlock);
  }

  public void runBlockCheck() throws Exception {
    this.blockCheck.runBlockCheck();
  }

}
