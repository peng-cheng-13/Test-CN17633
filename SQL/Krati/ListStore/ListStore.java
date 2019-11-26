/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.io.Closeable;
import krati.store.DynamicDataStore;
import krati.store.SerializableObjectStore;
import krati.io.serializer.StringSerializer;
import krati.io.serializer.JavaSerializer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The DataBase that handles block index info.
 */
public class ListStore implements Closeable {

  private final SerializableObjectStore<String, List<String>> mListStore;

  /**
   * Creates a new instance of {@link ListStore}.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  public ListStore(File homeDir, int initialCapacity) throws Exception {
    mListStore = createDataStore(homeDir, initialCapacity);
  }

  /**
   * @return the ListStore
   */
  public final SerializableObjectStore<String, List<String>> getDataStore() {
    return mListStore;
  }

  /**
   * Create the ListStore.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  protected SerializableObjectStore<String, List<String>>
      createDataStore(File homeDir, int initialCapacity) throws Exception {
    StoreConfig config = new StoreConfig(homeDir, initialCapacity);
    config.setSegmentFactory(new MemorySegmentFactory());
    config.setSegmentFileSizeMB(64);
    DynamicDataStore tmpstore = StoreFactory.createDynamicDataStore(config);
    SerializableObjectStore objectstore = new SerializableObjectStore(tmpstore,
        new StringSerializer(), new JavaSerializer<ArrayList>());
    return objectstore;
  }

  /**
   * Populate init data.
   */
  public void populate(int size, String keyprefix, String valueprefix) throws Exception {
    List<String> valueset = new ArrayList();
    for (int i = 0; i < size; i++) {
      valueset.add(valueprefix + i);
    }
    for (int i = 0; i < size; i++) {
      String tmpkey = keyprefix + i;
      //System.out.println("Put key : " + tmpkey + " to Hash store");
      putUDMPath(tmpkey, valueset);
    }
    valueset = null;
  }




  /**
   * Put the valueset corresponding to each key.
   * @param ikey the input key
   * @param valuelist the value list
   */
  public void putUDMKey(List<String> ikey, List<String> valuelist) throws Exception {
    int i;
    for (i = 0; i < ikey.size(); i++) {
      String tmpkey = ikey.get(i);
      if (mListStore.get(tmpkey) != null) {
        List<String> currentvalue = mListStore.get(tmpkey);
        currentvalue.add(valuelist.get(i));
        mListStore.put(tmpkey, currentvalue);
      } else {
        List<String> tmpvalue = new ArrayList();
        tmpvalue.add(valuelist.get(i));
        mListStore.put(tmpkey, tmpvalue);
      }
    }
  }

  /**
   * Put the UDM Path.
   * @param tvalue the key of each path
   * @param pathset the path list
   */
  public void putUDMPath(String tvalue, List<String> pathset) throws Exception {
    if (mListStore.get(tvalue) != null) {
      List<String> currentpath = mListStore.get(tvalue);
      Iterator<String> iterator = pathset.iterator();
      while (iterator.hasNext()) {
        currentpath.add(iterator.next());
      }
      mListStore.put(tvalue, currentpath);
    } else {
      mListStore.put(tvalue, pathset);
    }
  }

  /**
   * Delete the key value.
   * @param ikey the input key
   * @param valuelist the input value
   */
  public void deleteUDMKey(List<String> ikey, List<String> valuelist) throws Exception {
    int i;
    for (i = 0; i < ikey.size(); i++) {
      String tmpkey = ikey.get(i);
      if (mListStore.get(tmpkey) != null) {
        List<String> currentvalue = mListStore.get(tmpkey);
        currentvalue.remove(valuelist.get(i));
        mListStore.put(tmpkey, currentvalue);
      }
    }
  }

  /**
   * Delete the UDM Path.
   * @param tvalue the key of each path
   * @param pathset the path list
   */
  public void deleteUDMPath(String tvalue, List<String> pathset) throws Exception {
    if (mListStore.get(tvalue) != null) {
      List<String> currentpath = mListStore.get(tvalue);
      Iterator<String> iterator = pathset.iterator();
      while (iterator.hasNext()) {
        currentpath.remove(iterator.next());
      }
      mListStore.put(tvalue, currentpath);
    }
  }

  /**
   * Get the value of target key.
   * @param tkey the target key
   * @return the target value
   */
  public List<String> get(String tkey) {
    return mListStore.get(tkey);
  }

  /**
   * Sync the hash data base.
   */
  public void sync() throws Exception {
    mListStore.sync();
  }

  @Override
  public boolean isOpen() {
    return mListStore.isOpen();
  }

  @Override
  public void open() throws IOException {
    mListStore.open();
  }

  /**
   * Close the data base.
   */
  public void close() throws IOException {
    mListStore.close();
  }

}
