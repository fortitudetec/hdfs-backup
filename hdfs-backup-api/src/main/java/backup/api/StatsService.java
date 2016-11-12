package backup.api;

import java.io.IOException;

public interface StatsService<T extends Stats> {
  T getStats() throws IOException;
}
