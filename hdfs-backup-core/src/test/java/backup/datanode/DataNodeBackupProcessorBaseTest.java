package backup.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import backup.store.ExtendedBlock;

public class DataNodeBackupProcessorBaseTest {

  private static Configuration CONF;

  @BeforeClass
  public static void setupClass() {
    CONF = new Configuration();
  }

  @Test
  public void testSingleBlock() throws Exception {
    long blockId = 1;
    long length = 1;
    long generationStamp = 1;
    ExtendedBlockTestEntry extendedBlock = new ExtendedBlockTestEntry("p", blockId, length, generationStamp, 0);
    List<ExtendedBlockTestEntry> results = Arrays.asList(extendedBlock);
    try (TestDataNodeBackupProcessorBase test = new TestDataNodeBackupProcessorBase(CONF, results)) {
      test.enqueueBackup(extendedBlock.extendedBlock);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      assertEquals(0, test.getErrors()
                          .size());
      assertFalse(test.getIterator()
                      .hasNext());
    }
  }

  @Test
  public void testMultipleBlock() throws Exception {
    long length = 1;
    long generationStamp = 1;
    ExtendedBlockTestEntry eb1 = new ExtendedBlockTestEntry("p", 1, length, generationStamp, 0);
    ExtendedBlockTestEntry eb2 = new ExtendedBlockTestEntry("p", 2, length, generationStamp, 0);
    List<ExtendedBlockTestEntry> results = Arrays.asList(eb1, eb2);
    try (TestDataNodeBackupProcessorBase test = new TestDataNodeBackupProcessorBase(CONF, results)) {
      test.enqueueBackup(eb1.extendedBlock);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      test.enqueueBackup(eb2.extendedBlock);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      assertEquals(0, test.getErrors()
                          .size());
      assertFalse(test.getIterator()
                      .hasNext());
    }
  }

  @Test
  public void testMultipleBlockSameBlockId() throws Exception {
    long generationStamp = 1;
    ExtendedBlockTestEntry eb1 = new ExtendedBlockTestEntry("p", 1, 1, generationStamp, 0);
    ExtendedBlockTestEntry eb2 = new ExtendedBlockTestEntry("p", 1, 10, generationStamp, 0);
    List<ExtendedBlockTestEntry> results = Arrays.asList(eb2);
    try (TestDataNodeBackupProcessorBase test = new TestDataNodeBackupProcessorBase(CONF, results)) {
      test.enqueueBackup(eb1.extendedBlock, eb2.extendedBlock);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      assertEquals(0, test.getErrors()
                          .size());
      assertFalse(test.getIterator()
                      .hasNext());
    }
  }

  static class ExtendedBlockTestEntry {
    ExtendedBlock extendedBlock;
    long retryTime;

    ExtendedBlockTestEntry(String poolId, long blockId, long length, long generationStamp, long retryTime) {
      this.extendedBlock = new ExtendedBlock(poolId, blockId, length, generationStamp);
      this.retryTime = retryTime;
    }
  }

  private static class TestDataNodeBackupProcessorBase extends DataNodeBackupProcessorBase {

    private final Iterator<ExtendedBlockTestEntry> _iterator;
    private final List<Exception> _errors = new ArrayList<>();
    private final Object _lock = new Object();

    public TestDataNodeBackupProcessorBase(Configuration conf, List<ExtendedBlockTestEntry> results) throws Exception {
      super(conf);
      _iterator = results.iterator();
    }

    @Override
    protected long doBackup(ExtendedBlock extendedBlock, boolean force) throws Exception {
      synchronized (_lock) {
        try {
          if (!_iterator.hasNext()) {
            throw new RuntimeException("Too many extended blocks " + extendedBlock);
          }
          ExtendedBlockTestEntry eb = _iterator.next();
          if (!eb.extendedBlock.equals(extendedBlock)) {
            throw new RuntimeException("block " + eb + " does not equal " + extendedBlock);
          }
          return eb.retryTime;
        } catch (Exception e) {
          _errors.add(e);
          throw e;
        }
      }
    }

    public Iterator<ExtendedBlockTestEntry> getIterator() {
      synchronized (_lock) {
        return _iterator;
      }
    }

    public List<Exception> getErrors() {
      synchronized (_lock) {
        return _errors;
      }
    }

  }

}
