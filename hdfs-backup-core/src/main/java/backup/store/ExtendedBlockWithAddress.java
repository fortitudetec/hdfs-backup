package backup.store;

import backup.namenode.NameNodeBackupBlockCheckProcessor.Addresses;
import backup.store.ExtendedBlock;

public class ExtendedBlockWithAddress {
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