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
package backup;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

public interface Executable {

  public static Runnable createDaemon(Logger logger, long pauseOnError, AtomicBoolean running, Executable executable) {
    return new Runnable() {
      @Override
      public void run() {
        while (running.get()) {
          try {
            executable.run();
          } catch (Throwable t) {
            if (running.get()) {
              logger.error("unknown error", t);
              try {
                Thread.sleep(pauseOnError);
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }
      }
    };
  }

  void run() throws Exception;

}
