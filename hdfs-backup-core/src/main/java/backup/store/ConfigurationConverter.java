package backup.store;

import java.util.Map.Entry;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

public class ConfigurationConverter {

  public static org.apache.commons.configuration.Configuration convert(Configuration conf) {
    BaseConfiguration baseConfiguration = new BaseConfiguration();
    for (Entry<String, String> e : conf) {
      baseConfiguration.setProperty(e.getKey(), e.getValue());
    }
    return baseConfiguration;
  }

}
