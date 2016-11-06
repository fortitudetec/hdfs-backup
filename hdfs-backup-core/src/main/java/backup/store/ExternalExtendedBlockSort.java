/*
 * Copyright 2016 Fortitude Technologies LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.ImmutableSet;

public class ExternalExtendedBlockSort<T extends Writable> implements Closeable {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path dir = new Path("file:///home/apm/Development/git-projects/hdfs-backup/hdfs-backup-core/tmp");
    dir.getFileSystem(conf).delete(dir, true);
    long start = System.nanoTime();
    try (ExternalExtendedBlockSort<NullWritable> sort = new ExternalExtendedBlockSort<NullWritable>(conf, dir,
        NullWritable.class)) {
      Random random = new Random();
      for (int bp = 0; bp < 1; bp++) {
        String bpid = UUID.randomUUID().toString();
        for (int i = 0; i < 10000000; i++) {
          // for (int i = 0; i < 10; i++) {
          long genstamp = random.nextInt(20000);
          ExtendedBlock extendedBlock = new ExtendedBlock(bpid, random.nextLong(), random.nextInt(Integer.MAX_VALUE),
              genstamp);
          sort.add(extendedBlock, NullWritable.get());
        }
      }

      sort.finished();
      for (String blockPoolId : sort.getBlockPoolIds()) {
        ExtendedBlockEnum<NullWritable> blockEnum = sort.getBlockEnum(blockPoolId);
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
  private final Path baseDir;
  private final Class<? extends Writable> dataClass;

  public ExternalExtendedBlockSort(Configuration conf, Path baseDir, Class<? extends Writable> dataClass)
      throws IOException {
    this.conf = conf;
    this.baseDir = baseDir;
    this.dataClass = dataClass;
  }

  public void add(ExtendedBlock extendedBlock, Writable data) throws IOException {
    Writer writer = getWriter(extendedBlock.getPoolId());
    writer.append(
        new ComparableBlock(extendedBlock.getBlockId(), extendedBlock.getLength(), extendedBlock.getGenerationStamp()),
        data);
  }

  private Writer getWriter(String blockPoolId) throws IOException {
    Writer writer = writers.get(blockPoolId);
    if (writer != null) {
      return writer;
    }
    Path input = getInputFilePath(blockPoolId);
    writer = SequenceFile.createWriter(conf, Writer.file(input), Writer.keyClass(ComparableBlock.class),
        Writer.valueClass(dataClass));
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

  public ExtendedBlockEnum<T> getBlockEnum(String blockPoolId) throws Exception {
    Path output = getOutputFilePath(blockPoolId);
    FileSystem fileSystem = output.getFileSystem(conf);
    if (!fileSystem.exists(output)) {
      return null;
    }
    return new BlockEnum(blockPoolId, new Reader(conf, Reader.file(output)));
  }

  public class BlockEnum implements ExtendedBlockEnum<T> {
    private final Reader reader;
    private final String blockPoolId;
    private ExtendedBlock current;
    private T value;

    public BlockEnum(String blockPoolId, Reader reader) throws Exception {
      this.blockPoolId = blockPoolId;
      this.reader = reader;
      this.value = getValueInstance(reader);
    }

    @SuppressWarnings("unchecked")
    private T getValueInstance(Reader reader) throws InstantiationException, IllegalAccessException {
      Class<?> valueClass = reader.getValueClass();
      T val;
      if (valueClass == NullWritable.class) {
        val = (T) NullWritable.get();
      } else {
        val = (T) reader.getValueClass().newInstance();
      }
      return val;
    }

    @Override
    public ExtendedBlock current() {
      return current;
    }

    @Override
    public ExtendedBlock next() throws IOException {
      ComparableBlock block = new ComparableBlock();
      if (reader.next(block, value)) {
        return current = block.getExtendedBlock(blockPoolId);
      } else {
        return current = null;
      }
    }

    @Override
    public T currentValue() {
      return value;
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
        SequenceFile.Sorter sorter = new Sorter(local, ComparableBlock.class, dataClass, conf);
        sorter.sort(input, output);
      }
    }
  }

}
