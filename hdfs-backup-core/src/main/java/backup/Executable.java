package backup;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

public interface Executable {

  public static Runnable createDaemon(Logger logger, long pauseOnError, AtomicBoolean running, Executable executable) {
    return new Runnable() {
      @Override
      public void run() {
        while (running.get()) {
          try {
            executable.run();
          } catch (Throwable t) {
            if (running.get()) {
              logger.error("unknown error", t);
              try {
                Thread.sleep(pauseOnError);
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }
      }
    };
  }

  void run() throws Exception;

}
