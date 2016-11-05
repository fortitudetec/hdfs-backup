package backup.datanode;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolInfo;

import backup.store.WritableExtendedBlock;

@ProtocolInfo(protocolName = "BackupRPC", protocolVersion = 1)
public interface BackupRPC {

  void backupBlock(WritableExtendedBlock extendedBlock) throws IOException;
  
  void restoreBlock(WritableExtendedBlock extendedBlock) throws IOException;

}
