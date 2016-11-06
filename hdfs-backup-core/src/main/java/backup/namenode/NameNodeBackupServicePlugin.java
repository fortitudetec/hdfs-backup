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
package backup.namenode;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ServicePlugin;

import backup.SingletonManager;

public class NameNodeBackupServicePlugin extends Configured implements ServicePlugin {

  private NameNodeRestoreProcessor backupProcessor;

  @Override
  public void start(Object service) {
    NameNode namenode = (NameNode) service;
    // This object is created here so that it's lifecycle follows the namenode
    try {
      backupProcessor = SingletonManager.getManager(NameNodeRestoreProcessor.class).getInstance(namenode,
          () -> new NameNodeRestoreProcessor(getConf(), namenode));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    IOUtils.closeQuietly(backupProcessor);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

}
