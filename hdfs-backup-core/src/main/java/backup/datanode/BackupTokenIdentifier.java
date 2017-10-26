package backup.datanode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

public class BackupTokenIdentifier extends TokenIdentifier {

  private static final String BACKUP = "backup";
  private static final String BACKUP_TOKEN = "BACKUP_TOKEN";
  private static final Text TEXT = new Text(BACKUP_TOKEN);

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }

  @Override
  public Text getKind() {
    return TEXT;
  }

  @Override
  public UserGroupInformation getUser() {
    return UserGroupInformation.createRemoteUser(BACKUP);
  }

}
