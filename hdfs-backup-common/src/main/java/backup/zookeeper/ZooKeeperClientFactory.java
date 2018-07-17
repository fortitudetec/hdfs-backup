package backup.zookeeper;

import java.io.Closeable;
import java.io.IOException;

public interface ZooKeeperClientFactory extends Closeable {

  ZooKeeperClient getZk() throws IOException;

}
