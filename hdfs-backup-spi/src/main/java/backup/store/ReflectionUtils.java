package backup.store;

import org.apache.commons.configuration.Configuration;

public class ReflectionUtils {
  public static <T> T newInstance(Class<T> clazz, Configuration conf) throws Exception {
    T instance = clazz.newInstance();
    if (conf != null) {
      if (instance instanceof Configured) {
        Configured configured = (Configured) instance;
        configured.setConf(conf);
      }
    }
    return instance;
  }
}
