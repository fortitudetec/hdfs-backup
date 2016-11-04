package backup.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface ExtendedBlockEnum<T> extends Closeable {

  public static ExtendedBlockEnum<Void> EMPTY = new ExtendedBlockEnum<Void>() {

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

  public static ExtendedBlockEnum<Void> toExtendedBlockEnum(Iterator<ExtendedBlock> it) {
    return new ExtendedBlockEnum<Void>() {

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

  default T currentValue() {
    return null;
  }

  default public void close() throws IOException {

  }

}
