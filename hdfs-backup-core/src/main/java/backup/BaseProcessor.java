package backup;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseProcessor implements Runnable, Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(BaseProcessor.class);

  private final AtomicBoolean running = new AtomicBoolean(true);
  private Thread thread;

  protected void start() {
    thread = new Thread(this);
    thread.setDaemon(true);
    thread.setName(getClass().getName());
    thread.start();
  }

  protected boolean isRunning() {
    return running.get();
  }

  @Override
  public final void close() {
    running.set(false);
    closeInternal();
    thread.interrupt();
  }

  protected abstract void closeInternal();

  @Override
  public final void run() {
    try {
      initInternal();
    } catch (Throwable t) {
      if (isRunning()) {
        LOG.error("unknown error", t);
      }
    }
    while (isRunning()) {
      try {
        runInternal();
      } catch (Throwable t) {
        if (isRunning()) {
          LOG.error("unknown error", t);
        }
      }
    }
  }

  protected void initInternal() throws Exception {
  }

  protected abstract void runInternal() throws Exception;
}
