package backup.util;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;

public class HdfsUtils {

  public static boolean isActiveNamenode(Configuration configuration) throws IOException {
    try {
      String nn = getActiveNamenode(configuration);
      String hostName = Inet4Address.getLocalHost()
                                    .getHostName();
      if (hostName.equals(nn)) {
        return true;
      }
    } catch (NotHAException e) {
      return true;
    }
    return false;
  }

  public static String getActiveNamenode(Configuration configuration) throws IOException {
    Collection<String> nameservices = configuration.getStringCollection("dfs.nameservices");
    if (nameservices == null) {
      throw new NotHAException();
    }
    String nameService = nameservices.iterator()
                                     .next();
    Collection<String> namenodeIds = configuration.getStringCollection("dfs.ha.namenodes." + nameService);
    List<String> namenodeServerPortList = new ArrayList<>();
    for (String namenodeId : namenodeIds) {
      namenodeServerPortList.add(configuration.get("dfs.namenode.rpc-address." + "hdfs-sigma-dev" + "." + namenodeId));
    }
    for (String namenodeServerPort : namenodeServerPortList) {
      Path path = new Path("hdfs://" + namenodeServerPort + "/");
      FileSystem fileSystem = path.getFileSystem(configuration);
      try {
        fileSystem.getFileStatus(path);
        int indexOf = namenodeServerPort.indexOf(':');
        if (indexOf < 0) {
          return namenodeServerPort;
        } else {
          return namenodeServerPort.substring(0, indexOf);
        }
      } catch (RemoteException e) {
        RpcErrorCodeProto errorCode = e.getErrorCode();
        if (errorCode != RpcErrorCodeProto.ERROR_APPLICATION) {
          throw e;
        }
      }
    }
    return null;
  }

}
