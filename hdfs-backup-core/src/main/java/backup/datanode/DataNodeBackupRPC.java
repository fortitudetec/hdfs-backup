package backup.datanode;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolInfo;

import backup.store.WritableExtendedBlock;

@ProtocolInfo(protocolName = "DataNodeBackupRPC", protocolVersion = 1)
public interface DataNodeBackupRPC {

  void backupBlock(WritableExtendedBlock extendedBlock) throws IOException;

  void restoreBlock(WritableExtendedBlock extendedBlock) throws IOException;

}
