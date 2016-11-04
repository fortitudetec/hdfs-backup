package backup.integration;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import backup.IntegrationTestBase;
import junit.framework.JUnit4TestAdapter;
import junit.framework.Test;
import junit.framework.TestSuite;

public class BackupStoreIntTestSuite extends TestSuite {

  private static final String JAR = "jar";
  private static final String TESTS = "tests";
  private static File output;

  static {
    output = new File("./target/tmp_parcels");
    try {
      FileUtils.deleteDirectory(output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    output.mkdirs();
  }

  public static Test suite() throws Exception {
    // LocalRepository localRepo = new
    // LocalRepository("<REPO>");
    TestSuite suite = new TestSuite();
    addTests(suite, localRepo, "local-backup-store");
    addTests(suite, localRepo, "s3-backup-store");
    return suite;
  }

  private static void addTests(TestSuite suite, LocalRepository localRepo, String artifactId) throws Exception {
    List<IntegrationTestBase> integrationTestBases = getIntegrationTestBases(localRepo, artifactId, getVersion());
    Builder<String> builder = ImmutableList.builder();
    for (IntegrationTestBase integrationTestBase : integrationTestBases) {
      File parcelDir = extract(integrationTestBase);
      builder.add(parcelDir.getAbsolutePath());
      MiniClusterTestBase.addIntegrationTestBase(integrationTestBase);
      JUnit4TestAdapter test = new JUnit4TestAdapter(MiniClusterTestBase.class);
      suite.addTest(test);
    }
    System.setProperty("HDFS_BACKUP_PLUGINS_PROP", Joiner.on(':')
                                                         .join(builder.build()));
  }

  private static File extract(IntegrationTestBase integrationTestBase) throws Exception {
    File parcelLocation = integrationTestBase.getParcelLocation();
    try (TarArchiveInputStream tarInput = new TarArchiveInputStream(
        new GzipCompressorInputStream(new FileInputStream(parcelLocation)))) {
      ArchiveEntry entry;
      while ((entry = (ArchiveEntry) tarInput.getNextEntry()) != null) {
        System.out.println("Extracting: " + entry.getName());
        File f = new File(output, entry.getName());
        if (entry.isDirectory()) {
          f.mkdirs();
        } else {
          f.getParentFile()
           .mkdirs();
          try (OutputStream out = new BufferedOutputStream(new FileOutputStream(f))) {
            IOUtils.copy(tarInput, out);
          }
        }
      }
    }
    return findLibDir(output);
  }

  private static File findLibDir(File file) {
    if (file.isDirectory()) {
      if (file.getName()
              .equals("lib")) {
        return file;
      }
      for (File f : file.listFiles()) {
        File libDir = findLibDir(f);
        if (libDir != null) {
          return libDir;
        }
      }
    }
    return null;
  }

  public static List<IntegrationTestBase> getIntegrationTestBases(LocalRepository localRepo, String artifactId,
      String version) throws Exception {

    DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
    locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
    locator.addService(TransporterFactory.class, FileTransporterFactory.class);
    locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
    RepositorySystem system = locator.getService(RepositorySystem.class);
    DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

    Artifact jarArtifact = getArtifact(system, session,
        new DefaultArtifact(getGroupName(), artifactId, TESTS, JAR, version));
    Artifact tarArtifact = getArtifact(system, session,
        new DefaultArtifact(getGroupName(), artifactId, "tar.gz", version));

    ClassLoader classLoader = new URLClassLoader(new URL[] { jarArtifact.getFile()
                                                                        .toURI()
                                                                        .toURL() });
    ServiceLoader<IntegrationTestBase> loader = ServiceLoader.load(IntegrationTestBase.class, classLoader);
    List<IntegrationTestBase> list = new ArrayList<>();
    loader.forEach(b -> {
      b.setParcelLocation(tarArtifact.getFile());
      list.add(b);
    });
    return list;
  }

  private static Artifact getArtifact(RepositorySystem system, DefaultRepositorySystemSession session,
      DefaultArtifact rartifact) throws ArtifactResolutionException {
    ArtifactRequest artifactRequest = new ArtifactRequest();
    artifactRequest.setArtifact(rartifact);
    ArtifactResult artifactResult = system.resolveArtifact(session, artifactRequest);
    Artifact artifact = artifactResult.getArtifact();
    return artifact;
  }

  private static String getGroupName() {
    return "hdfs-backup";
  }

  private static String getVersion() {
    return "1.0";
  }

}
