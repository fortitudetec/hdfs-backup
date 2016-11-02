package backup.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.google.common.collect.ImmutableSet;

public class ExternalExtendedBlockSort implements Closeable {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path dir = new Path("file:///home/apm/Development/git-projects/hdfs-backup/hdfs-backup-core/tmp");
    dir.getFileSystem(conf)
       .delete(dir, true);
    long start = System.nanoTime();
    try (ExternalExtendedBlockSort sort = new ExternalExtendedBlockSort(conf, dir)) {
      Random random = new Random();
      for (int bp = 0; bp < 1; bp++) {
        String bpid = UUID.randomUUID()
                          .toString();
        for (int i = 0; i < 10000000; i++) {
          // for (int i = 0; i < 10; i++) {
          long genstamp = random.nextInt(20000);
          ExtendedBlock extendedBlock = new ExtendedBlock(bpid, random.nextLong(), random.nextInt(Integer.MAX_VALUE),
              genstamp);
          sort.add(extendedBlock);
        }
      }

      sort.finished();
      for (String blockPoolId : sort.getBlockPoolIds()) {
        ExtendedBlockEnum blockEnum = sort.getBlockEnum(blockPoolId);
        ExtendedBlock block;
        long l = 0;
        while ((block = blockEnum.next()) != null) {
          // System.out.println(block);
          l += block.getBlockId();
        }
        System.out.println(l);
      }
    }
    long end = System.nanoTime();
    System.out.println("Time [" + (end - start) / 1000000.0 + " ms]");
  }

  private final Configuration conf;
  private final Map<String, Writer> writers = new HashMap<>();
  private final NullWritable value;
  private final Path baseDir;

  public ExternalExtendedBlockSort(Configuration conf, Path baseDir) throws IOException {
    this.conf = conf;
    this.baseDir = baseDir;
    value = NullWritable.get();
  }

  public void add(ExtendedBlock extendedBlock) throws IOException {
    Writer writer = getWriter(extendedBlock.getBlockPoolId());
    writer.append(new ComparableBlock(extendedBlock.getBlockId(), extendedBlock.getNumBytes(),
        extendedBlock.getGenerationStamp()), value);
  }

  private Writer getWriter(String blockPoolId) throws IOException {
    Writer writer = writers.get(blockPoolId);
    if (writer != null) {
      return writer;
    }
    Path input = getInputFilePath(blockPoolId);
    writer = SequenceFile.createWriter(conf, Writer.file(input), Writer.keyClass(ComparableBlock.class),
        Writer.valueClass(NullWritable.class));
    writers.put(blockPoolId, writer);
    return writer;
  }

  private Path getInputFilePath(String blockPoolId) {
    Path path = new Path(baseDir, blockPoolId);
    return new Path(path, "input.seq");
  }

  private Path getOutputFilePath(String blockPoolId) {
    Path path = new Path(baseDir, blockPoolId);
    return new Path(path, "output.seq");
  }

  public void finished() throws IOException {
    for (Writer writer : writers.values()) {
      IOUtils.closeQuietly(writer);
    }
    sortIfNeeded();
  }

  public Collection<String> getBlockPoolIds() {
    return ImmutableSet.copyOf(writers.keySet());
  }

  public ExtendedBlockEnum getBlockEnum(String blockPoolId) throws IOException {
    Path output = getOutputFilePath(blockPoolId);
    FileSystem fileSystem = output.getFileSystem(conf);
    if (!fileSystem.exists(output)) {
      return null;
    }
    return new BlockEnum(blockPoolId, new Reader(conf, Reader.file(output)));
  }

  public class BlockEnum implements ExtendedBlockEnum {
    private final Reader reader;
    private final String blockPoolId;

    private ExtendedBlock current;

    public BlockEnum(String blockPoolId, Reader reader) {
      this.blockPoolId = blockPoolId;
      this.reader = reader;
    }

    public ExtendedBlock current() {
      return current;
    }

    public ExtendedBlock next() throws IOException {
      ComparableBlock block = new ComparableBlock();
      if (reader.next(block)) {
        return current = block.getExtendedBlock(blockPoolId);
      } else {
        return current = null;
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeQuietly(reader);
    }
  }

  @Override
  public void close() throws IOException {
    for (Writer writer : writers.values()) {
      IOUtils.closeQuietly(writer);
    }
    FileSystem fileSystem = baseDir.getFileSystem(conf);
    fileSystem.delete(baseDir, true);
  }

  private synchronized void sortIfNeeded() throws IOException {
    for (String blockPoolId : writers.keySet()) {
      Path output = getOutputFilePath(blockPoolId);
      Path input = getInputFilePath(blockPoolId);
      FileSystem fileSystem = output.getFileSystem(conf);
      if (!fileSystem.exists(output) && fileSystem.exists(input)) {
        LocalFileSystem local = FileSystem.getLocal(conf);
        SequenceFile.Sorter sorter = new Sorter(local, ComparableBlock.class, NullWritable.class, conf);
        sorter.sort(input, output);
      }
    }
  }

}
