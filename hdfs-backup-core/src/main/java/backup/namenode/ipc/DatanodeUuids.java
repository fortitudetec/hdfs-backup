package backup.namenode.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class DatanodeUuids implements Writable {

  private static final String UTF_8 = "UTF-8";
  private List<String> datanodeUuids = new ArrayList<>();

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(datanodeUuids.size());
    for (String id : datanodeUuids) {
      writeString(id, out);
    }
  }

  private void writeString(String s, DataOutput out) throws IOException {
    byte[] bs = s.getBytes(UTF_8);
    out.writeInt(bs.length);
    out.write(bs);
  }

  private String readString(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] buf = new byte[size];
    in.readFully(buf);
    return new String(buf, UTF_8);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    datanodeUuids.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      datanodeUuids.add(readString(in));
    }
  }

  public void add(String datanodeUuid) {
    datanodeUuids.add(datanodeUuid);
  }

  public List<String> getDatanodeUuids() {
    return datanodeUuids;
  }

  public void setDatanodeUuids(List<String> datanodeUuids) {
    this.datanodeUuids = datanodeUuids;
  }

  @Override
  public String toString() {
    return "DatanodeUuids [datanodeUuids=" + datanodeUuids + "]";
  }

}
