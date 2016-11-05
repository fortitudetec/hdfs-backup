package backup.store;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

public class BackupStoreClassHelper {

  private final static Logger LOG = LoggerFactory.getLogger(BackupStoreClassHelper.class);
  private static final String HDFS_BACKUP_PLUGINS_PROP = "hdfs.backup.plugins";
  private static final String HDFS_BACKUP_PLUGINS_ENV = "HDFS_BACKUP_PLUGINS";

  public static void main(String[] args) throws Exception {
    tryToFindPlugin(args[0]);
  }

  private static final Map<File, ClassLoader> CLASS_LOADERS = new MapMaker().makeMap();

  static {
    List<String> pluginDirs = new ArrayList<>();
    pluginDirs.addAll(split(',', System.getProperty(HDFS_BACKUP_PLUGINS_PROP)));
    pluginDirs.addAll(split(':', System.getenv(HDFS_BACKUP_PLUGINS_ENV)));
    loadClassLoaders(pluginDirs);
  }

  public static void loadClassLoaders(String... pluginDir) {
    if (pluginDir == null) {
      return;
    }
    loadClassLoaders(Arrays.asList(pluginDir));
  }

  public static void loadClassLoaders(List<String> pluginDirs) {
    try {
      for (String pluginDir : pluginDirs) {
        File plugin = new File(pluginDir).getAbsoluteFile();
        if (CLASS_LOADERS.containsKey(plugin)) {
          LOG.info("Class Loader for path {} already loaded.", plugin);
          continue;
        }
        if (plugin.exists()) {
          CLASS_LOADERS.put(plugin, newClassLoader(plugin));
        } else {
          LOG.error("pluging dir {} missing", plugin);
        }
      }
    } catch (Exception e) {
      LOG.error("error trying to load plugins.", e);
      throw new RuntimeException(e);
    }
  }

  private static List<String> split(char on, String pluginDirsStr) {
    if (pluginDirsStr == null) {
      return ImmutableList.of();
    }
    Iterable<String> pluginDirs = Splitter.on(on).split(pluginDirsStr);
    return ImmutableList.copyOf(pluginDirs);
  }

  public static Class<?> tryToFindPlugin(String classname) throws Exception {
    for (Entry<File, ClassLoader> entry : CLASS_LOADERS.entrySet()) {
      File file = entry.getKey();
      ClassLoader classLoader = entry.getValue();
      try {
        return classLoader.loadClass(classname);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        LOG.info("Class {} not found in {} {}", classname, file, classLoader);
      }
    }
    throw new ClassNotFoundException(classname);
  }

  private static ClassLoader newClassLoader(File pluginDir) throws Exception {
    LOG.info("loading new plugin {}", pluginDir);
    List<URL> urls = new ArrayList<>();
    for (File file : pluginDir.listFiles()) {
      urls.add(file.toURI().toURL());
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
