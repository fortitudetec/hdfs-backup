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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class BackupUtil {

  private static final ThreadLocal<Random> random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  public static Random getRandom() {
    return random.get();
  }

  public static int nextInt(int bound) {
    if (bound == 1) {
      return 0;
    }
    return getRandom().nextInt(bound);
  }

  public static String[] getIpAddrs(DatanodeInfo[] locations) {
    String[] result = new String[locations.length];
    for (int i = 0; i < locations.length; i++) {
      result[i] = locations[i].getIpAddr();
    }
    return result;
  }

  public static int[] getIpcPorts(DatanodeInfo[] locations) {
    int[] result = new int[locations.length];
    for (int i = 0; i < locations.length; i++) {
      result[i] = locations[i].getIpcPort();
    }
    return result;
  }

  public static org.apache.commons.configuration.Configuration convert(Configuration conf) {
    BaseConfiguration baseConfiguration = new BaseConfiguration();
    for (Entry<String, String> e : conf) {
      baseConfiguration.setProperty(e.getKey(), e.getValue());
    }
    return baseConfiguration;
  }

  public static org.apache.hadoop.hdfs.protocol.ExtendedBlock toHadoop(ExtendedBlock block) {
    if (block == null) {
      return null;
    }
    return new org.apache.hadoop.hdfs.protocol.ExtendedBlock(block.getPoolId(), block.getBlockId(), block.getLength(),
        block.getGenerationStamp());
  }

  public static ExtendedBlock fromHadoop(org.apache.hadoop.hdfs.protocol.ExtendedBlock block) {
    if (block == null) {
      return null;
    }
    return new ExtendedBlock(block.getBlockPoolId(), block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp());
  }

  public static String readShortString(DataInput in) throws IOException {
    byte[] buf = new byte[in.readShort()];
    in.readFully(buf);
    return new String(buf);
  }

  public static void writeShortString(String s, DataOutput out) throws IOException {
    int length = s.length();
    byte[] bs = s.getBytes();
    out.writeShort(length);
    out.write(bs);
  }
}
