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

import java.util.Map;

import com.google.common.collect.MapMaker;

public class SingletonManager<T> {

  private static final Map<Class<?>, SingletonManager<?>> INSTANCES = new MapMaker().makeMap();

  @SuppressWarnings("unchecked")
  public static synchronized <T> SingletonManager<T> getManager(Class<T> clazz) {
    SingletonManager<?> singleton = INSTANCES.get(clazz);
    if (singleton == null) {
      INSTANCES.put(clazz, singleton = new SingletonManager<>());
    }
    return (SingletonManager<T>) singleton;
  }

  private final Map<Object, Object> instances = new MapMaker().makeMap();

  @SuppressWarnings("unchecked")
  public synchronized T getInstance(Object key, Creator<T> creator) throws Exception {
    Object instance = instances.get(key);
    if (instance == null) {
      instances.put(key, instance = creator.create());
    }
    return (T) instance;
  }

  @SuppressWarnings("unchecked")
  public synchronized T getInstance(Object key) throws Exception {
    return (T) instances.get(key);
  }

  public static interface Creator<K> {
    K create() throws Exception;
  }

}
