package backup.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import backup.namenode.ipc.DatanodeUuids;
import backup.namenode.ipc.NameNodeBackupRPC;

public class NameNodeClient implements NameNodeBackupRPC {

  private static final Logger LOGGER = LoggerFactory.getLogger(NameNodeClient.class);

  private final List<NameNodeBackupRPC> _nameNodeClients;

  public NameNodeClient(Configuration configuration, UserGroupInformation ugi) throws Exception {
    Map<String, Map<String, InetSocketAddress>> newAddressMap = DFSUtil.getNNServiceRpcAddressesForCluster(
        configuration);
    Builder<NameNodeBackupRPC> builder = ImmutableList.builder();
    for (Map<String, InetSocketAddress> map : newAddressMap.values()) {
      for (InetSocketAddress address : map.values()) {
        builder.add(NameNodeBackupRPC.getDataNodeBackupRPC(address, configuration, ugi));
      }
    }
    _nameNodeClients = builder.build();
  }

  @Override
  public DatanodeUuids getDatanodeUuids(String poolId, Block block) throws IOException {
    IOException lastException = null;
    List<DatanodeUuids> results = new ArrayList<>();
    for (NameNodeBackupRPC client : _nameNodeClients) {
      try {
        results.add(client.getDatanodeUuids(poolId, block));
      } catch (IOException e) {
        lastException = e;
      }
    }
    if (results.isEmpty() && lastException != null) {
      LOGGER.error("Unknown error while trying to get datanode uuids for block {}", block);
      throw lastException;
    }
    return merge(results);
  }

  private DatanodeUuids merge(List<DatanodeUuids> results) {
    Set<String> result = new HashSet<>();
    for (DatanodeUuids datanodeUuids : results) {
      result.addAll(datanodeUuids.getDatanodeUuids());
    }
    DatanodeUuids datanodeUuids = new DatanodeUuids();
    datanodeUuids.setDatanodeUuids(new ArrayList<>(result));
    return datanodeUuids;
  }

}
