package backup.store;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class BackupStoreClassHelper {

  private final static Logger LOG = LoggerFactory.getLogger(BackupStoreClassHelper.class);

  private static final String HDFS_BACKUP_PLUGINS = "HDFS_BACKUP_PLUGINS";

  public static void main(String[] args) throws Exception {
    tryToFindPlugin(args[0]);
  }

  private static final List<ClassLoader> CLASS_LOADERS;

  static {
    String pluginDirsStr = System.getenv(HDFS_BACKUP_PLUGINS);
    Iterable<String> pluginDirs = Splitter.on(':')
                                          .split(pluginDirsStr);
    try {
      Builder<ClassLoader> builder = ImmutableList.builder();
      for (String pluginDir : pluginDirs) {
        File plugin = new File(pluginDir);
        if (plugin.exists()) {
          builder.add(newClassLoader(plugin));
        } else {
          LOG.error("pluging dir {} missing", plugin);
        }
      }
      CLASS_LOADERS = builder.build();
    } catch (Exception e) {
      LOG.error("error trying to load plugins.", e);
      throw new RuntimeException(e);
    }
  }

  public static Class<?> tryToFindPlugin(String classname) throws Exception {
    for (ClassLoader classLoader : CLASS_LOADERS) {
      try {
        return classLoader.loadClass(classname);
      } catch (ClassNotFoundException e) {
        LOG.info("Class {} not found in {}", classname, classLoader);
      }
    }
    throw new ClassNotFoundException(classname);
  }

  private static ClassLoader newClassLoader(File pluginDir) throws Exception {
    LOG.info("loading new plugin {}", pluginDir);
    List<URL> urls = new ArrayList<>();
    for (File file : pluginDir.listFiles()) {
      urls.add(file.toURI()
                   .toURL());
    }
    return new BackupStoreClassLoader(urls.toArray(new URL[] {}));
  }

  private static class BackupStoreClassLoader extends URLClassLoader {

    public BackupStoreClassLoader(URL[] urls) {
      super(urls);
    }

    public Class<?> loadClass(String name) throws ClassNotFoundException {
      return loadClass(name, false);
    }

    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      Class<?> clazz = findLoadedClass(name);
      if (clazz == null) {
        try {
          clazz = findClass(name);
        } catch (ClassNotFoundException cnfe) {
          // do nothing
        }
      }
      if (clazz == null) {
        ClassLoader parent = getParent();
        if (parent != null) {
          clazz = parent.loadClass(name);
        } else {
          clazz = getSystemClassLoader().loadClass(name);
        }
      }
      if (resolve) {
        resolveClass(clazz);
      }
      return clazz;
    }

    public URL getResource(String name) {
      URL url = findResource(name);
      if (url == null) {
        url = getParent().getResource(name);
      }
      return url;
    }
  }
}
