package backup.datanode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Meter {

  private final AtomicLong counter = new AtomicLong();

  private long prevTime;
  private long prevValue;
  private double countPerSecond;

  public AtomicLong getCounter() {
    return counter;
  }

  public synchronized double getCountPerSecond() {
    long now = System.nanoTime();
    if (prevTime + TimeUnit.SECONDS.toNanos(5) < now) {
      double seconds = (now - prevTime) / 1000000000.0;
      long currentValue = counter.get();
      long deltaValue = currentValue - prevValue;
      prevTime = now;
      prevValue = currentValue;
      return countPerSecond = (deltaValue / seconds);
    }
    return countPerSecond;
  }
}
