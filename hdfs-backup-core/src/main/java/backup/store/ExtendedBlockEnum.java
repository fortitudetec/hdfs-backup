package backup.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public interface ExtendedBlockEnum extends Closeable {

  public static ExtendedBlockEnum EMPTY = new ExtendedBlockEnum() {

    @Override
    public void close() throws IOException {

    }

    @Override
    public ExtendedBlock next() throws Exception {
      return null;
    }

    @Override
    public ExtendedBlock current() {
      return null;
    }
  };

  public static ExtendedBlockEnum toExtendedBlockEnum(Iterator<ExtendedBlock> it) {
    return new ExtendedBlockEnum() {

      private ExtendedBlock current;

      @Override
      public ExtendedBlock next() throws Exception {
        if (it.hasNext()) {
          return current = it.next();
        }
        return current = null;
      }

      @Override
      public ExtendedBlock current() {
        return current;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }

  ExtendedBlock current();

  ExtendedBlock next() throws Exception;

  default public void close() throws IOException {

  }

}
