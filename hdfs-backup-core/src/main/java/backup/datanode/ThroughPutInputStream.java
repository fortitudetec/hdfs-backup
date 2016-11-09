package backup.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

public class ThroughPutInputStream extends InputStream {

  private final InputStream input;
  private final AtomicLong bytesCounter;

  public ThroughPutInputStream(InputStream input, AtomicLong bytesCounter) {
    this.input = input;
    this.bytesCounter = bytesCounter;
  }

  @Override
  public int read() throws IOException {
    try {
      return input.read();
    } finally {
      bytesCounter.incrementAndGet();
    }
  }

  public int read(byte[] b) throws IOException {
    int read = input.read(b);
    bytesCounter.addAndGet(read);
    return read;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int read = input.read(b, off, len);
    bytesCounter.addAndGet(read);
    return read;
  }

  public long skip(long n) throws IOException {
    return input.skip(n);
  }

  public String toString() {
    return input.toString();
  }

  public int available() throws IOException {
    return input.available();
  }

  public void close() throws IOException {
    input.close();
  }

  public void mark(int readlimit) {
    input.mark(readlimit);
  }

  public void reset() throws IOException {
    input.reset();
  }

  public boolean markSupported() {
    return input.markSupported();
  }

}
