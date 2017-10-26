package backup.store;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LengthInputStream extends FilterInputStream {

  private final long length;
  private long current;

  public LengthInputStream(InputStream in, long length) {
    super(in);
    this.length = length;
  }

  public long getLength() {
    return length;
  }

  @Override
  public int read() throws IOException {
    if (current >= length) {
      return -1;
    }
    try {
      return super.read();
    } finally {
      current++;
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (current >= length) {
      return -1;
    }
    int l = (int) Math.min(len, length - current);
    int read = super.read(b, off, l);
    current += read;
    return read;
  }

  public InputStream getWrappedStream() {
    return in;
  }
}