package backup.store;

import java.io.File;
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

import classloader.FileClassLoader;

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
    return new FileClassLoader(pluginDir);
  }

}
