package backup.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

public class Closer implements Closeable {

  private final List<Closeable> list = new ArrayList<>();

  public static Closer create() {
    return new Closer();
  }

  public <T extends Closeable> T register(T closeable) {
    list.add(closeable);
    return closeable;
  }

  @Override
  public void close() throws IOException {
    for (Closeable closeable : list) {
      IOUtils.closeQuietly(closeable);
    }
  }

  public ExecutorService register(ExecutorService executor) {
    register((Closeable) () -> {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        throw new IOException(e);
      }
    });
    return executor;
  }

}
