package backup.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import backup.datanode.ipc.DataNodeBackupRPC;

public interface DataNodeBackupRPCLookup {

  DataNodeBackupRPC getRpc(InetSocketAddress dataNodeAddress) throws IOException, InterruptedException;

}
