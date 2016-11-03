package backup.store;

import java.io.FilterInputStream;
import java.io.InputStream;

public class LengthInputStream extends FilterInputStream {

  private final long length;

  public LengthInputStream(InputStream in, long length) {
    super(in);
    this.length = length;
  }

  public long getLength() {
    return length;
  }

  public InputStream getWrappedStream() {
    return in;
  }
}