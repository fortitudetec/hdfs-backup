package backup.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableUtil {
  public static String readShortString(DataInput in) throws IOException {
    byte[] buf = new byte[in.readShort()];
    in.readFully(buf);
    return new String(buf);
  }

  public static void writeShortString(String s, DataOutput out) throws IOException {
    int length = s.length();
    byte[] bs = s.getBytes();
    out.writeShort(length);
    out.write(bs);
  }
}
