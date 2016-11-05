package backup.store;

import java.util.Random;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class BackupUtil {

  private static final ThreadLocal<Random> random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  public static Random getRandom() {
    return random.get();
  }

  public static int nextInt(int bound) {
    if (bound == 1) {
      return 0;
    }
    return getRandom().nextInt(bound);
  }

  public static String[] getIpAddrs(DatanodeInfo[] locations) {
    String[] result = new String[locations.length];
    for (int i = 0; i < locations.length; i++) {
      result[i] = locations[i].getIpAddr();
    }
    return result;
  }

  public static int[] getIpcPorts(DatanodeInfo[] locations) {
    int[] result = new int[locations.length];
    for (int i = 0; i < locations.length; i++) {
      result[i] = locations[i].getIpcPort();
    }
    return result;
  }
}
