package backup.zookeeper;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class ZkUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

  public static final int ANY_VERSION = -1;

  public static class ConnectionWatcher implements Watcher {

    private String zkConnectionString;
    private int sessionTimeout;

    public void setZkConnectionString(String zkConnectionString) {
      this.zkConnectionString = zkConnectionString;
    }

    public void setSessionTimeout(int sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
    }

    @Override
    public void process(WatchedEvent event) {
      KeeperState state = event.getState();
      LOG.info("ZooKeeper [{}] timeout [{}] changed to [{}] state", zkConnectionString, sessionTimeout, state);
    }

  }

  public static void pause(Object o) {
    synchronized (o) {
      try {
        o.wait(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  public static ZooKeeperClient newZooKeeper(String zkConnectionString, int sessionTimeout) throws IOException {
    ConnectionWatcher watcher = new ConnectionWatcher();
    watcher.setSessionTimeout(sessionTimeout);
    watcher.setZkConnectionString(zkConnectionString);
    createChrootPathIfNeeded(zkConnectionString, sessionTimeout, watcher);
    return new ZooKeeperClient(zkConnectionString, sessionTimeout, watcher);
  }

  private static void createChrootPathIfNeeded(String zkConnectionString, int sessionTimeout, ConnectionWatcher watcher)
      throws IOException {
    int indexOf = zkConnectionString.indexOf('/');
    if (indexOf >= 0) {
      String baseConnection = zkConnectionString.substring(0, indexOf);
      try (ZooKeeperClient zk = new ZooKeeperClient(baseConnection, sessionTimeout, watcher)) {
        ZkUtils.mkNodesStr(zk, zkConnectionString.substring(indexOf));
      }
    }
  }

  public static void mkNodesStr(ZooKeeper zk, String path) {
    if (path == null) {
      return;
    }
    mkNodes(zk, path.split("/"));
  }

  public static void mkNodes(ZooKeeper zk, String... path) {
    if (path == null) {
      return;
    }
    for (int i = 0; i < path.length; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j <= i; j++) {
        if (!path[j].isEmpty()) {
          builder.append('/');
          builder.append(path[j]);
        }
      }
      String pathToCheck = removeDupSeps(builder.toString());
      if (pathToCheck.isEmpty()) {
        continue;
      }
      try {
        if (zk.exists(pathToCheck, false) == null) {
          zk.create(pathToCheck, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      } catch (NodeExistsException e) {
        // do nothing
      } catch (KeeperException e) {
        LOG.error("error", e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        LOG.error("error", e);
        throw new RuntimeException(e);
      }
    }
  }

  private static String removeDupSeps(String path) {
    return path.replace("//", "/");
  }

  public static String createPath(String... parts) {
    if (parts == null || parts.length == 0) {
      return null;
    }
    return "/" + trimSlash(Joiner.on('/').join(trimSlash(parts)));
  }

  private static String[] trimSlash(String[] parts) {
    for (int i = 0; i < parts.length; i++) {
      parts[i] = trimSlash(parts[i]);
    }
    return parts;
  }

  private static String trimSlash(String s) {
    int start = 0;
    for (int c = 0; c < s.length(); c++) {
      if (s.charAt(c) != '/') {
        break;
      }
      start++;
    }
    if (start >= s.length()) {
      return "";
    }
    int end = s.length();
    for (int c = end - 1; c >= 0; c--) {
      if (s.charAt(c) != '/') {
        break;
      }
      end--;
    }
    return s.substring(start, end);
  }

  public static boolean exists(ZooKeeper zk, String... path) {
    if (path == null || path.length == 0) {
      return true;
    }
    StringBuilder builder = new StringBuilder(path[0]);
    for (int i = 1; i < path.length; i++) {
      builder.append('/').append(path[i]);
    }
    try {
      return zk.exists(builder.toString(), false) != null;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteAnyVersion(ZooKeeper zk, String path) {
    try {
      List<String> children = zk.getChildren(path, false);
      for (String c : children) {
        deleteAnyVersion(zk, path + "/" + c);
      }
      zk.delete(path, ANY_VERSION);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        return;
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void waitUntilExists(ZooKeeper zooKeeper, String path) {
    final Object o = new Object();
    try {
      while (true) {
        Stat stat = zooKeeper.exists(path, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            synchronized (o) {
              o.notifyAll();
            }
          }
        });
        if (stat == null) {
          synchronized (o) {
            o.wait();
          }
        } else {
          return;
        }
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void rmr(ZooKeeper zooKeeper, String path) throws Exception {
    if (zooKeeper.exists(path, false) == null) {
      return;
    }
    List<String> children = zooKeeper.getChildren(path, false);
    for (String s : children) {
      rmr(zooKeeper, (path + "/" + s).replace("//", "/"));
    }
    if (path.equals("/")) {
      return;
    }
    zooKeeper.delete(path, -1);
  }

}