package backup.namenode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class NameNodeBackupBlockCheckProcessorTest {

  @Test
  public void testNameNodeBackupBlockCheckProcessorTest1() {
    Path path = new Path("/a/b/c/d/e/f");
    Set<Path> pathSetToIgnore = new HashSet<>();
    pathSetToIgnore.add(new Path("/a"));
    assertTrue((NameNodeBackupBlockCheckProcessor.shouldIgnore(pathSetToIgnore, path)));
  }

  @Test
  public void testNameNodeBackupBlockCheckProcessorTest2() {
    Path path = new Path("/a/b/c/d/e/f");
    Set<Path> pathSetToIgnore = new HashSet<>();
    pathSetToIgnore.add(new Path("/a/c"));
    assertFalse((NameNodeBackupBlockCheckProcessor.shouldIgnore(pathSetToIgnore, path)));
  }

  @Test
  public void testNameNodeBackupBlockCheckProcessorTest3() {
    Path path = new Path("/a/b/c/d/e/f");
    Set<Path> pathSetToIgnore = new HashSet<>();
    pathSetToIgnore.add(new Path("/a/b/c/d/e/f"));
    assertTrue((NameNodeBackupBlockCheckProcessor.shouldIgnore(pathSetToIgnore, path)));
  }

  @Test
  public void testNameNodeBackupBlockCheckProcessorTest4() {
    Path path = new Path("/a/b/c/d/e/f");
    Set<Path> pathSetToIgnore = new HashSet<>();
    pathSetToIgnore.add(new Path("/a/b/c/d/e/f/g"));
    assertFalse((NameNodeBackupBlockCheckProcessor.shouldIgnore(pathSetToIgnore, path)));
  }

  @Test
  public void testNameNodeBackupBlockCheckProcessorTest5() {
    Path path = new Path("/a/b/c/d/e/f");
    Set<Path> pathSetToIgnore = new HashSet<>();
    pathSetToIgnore.add(new Path("/a/b/c/e/e/f/g"));
    assertFalse((NameNodeBackupBlockCheckProcessor.shouldIgnore(pathSetToIgnore, path)));
  }

}
