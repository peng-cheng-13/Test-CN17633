import java.io.File;
import java.io.IOException;
import java.util.Random;

import krati.*;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.io.Closeable;
import krati.store.DataStore;
import krati.store.DynamicDataStore;
import krati.store.SerializableObjectStore;
import krati.io.serializer.*;


public class KratiDataStore implements Closeable {

  private final int _initialCapacity;
  private final SerializableObjectStore<String, IndexInfo> _store;

  public KratiDataStore(File homeDir, int initialCapacity) throws Exception {
    _initialCapacity = initialCapacity;
    _store = createDataStore(homeDir, initialCapacity);
  }

  public final SerializableObjectStore<String, IndexInfo> getDataStore() {
    return _store;
  }

  protected SerializableObjectStore<String, IndexInfo> createDataStore(File homeDir, int initialCapacity) throws Exception {
    StoreConfig config = new StoreConfig(homeDir, initialCapacity);
    config.setSegmentFactory(new MemorySegmentFactory());
    config.setSegmentFileSizeMB(64);
    DynamicDataStore tmpstore = StoreFactory.createDynamicDataStore(config);
    SerializableObjectStore objectstore = new SerializableObjectStore(tmpstore, new StringSerializer(), new JavaSerializer<IndexInfo>());
    return objectstore;
  }

  protected byte[] createDataForKey(String key) {
    return ("Here is your data for " + key).getBytes();
  }

  public void populate() throws Exception {
    IndexInfo tmp = new IndexInfo(100, 10, 1, "peng");
    for (int i = 0; i < _initialCapacity; i++) {
      String str = "key." + i;
      byte[] key = str.getBytes();
      byte[] value = createDataForKey(str);
      //_store.put(key, value);
      _store.put(str, tmp);
    }
    _store.sync();
  }

  public void doRandomReads(int readCnt) {
    Random rand = new Random();
    IndexInfo tmp;
    int length = _store.capacity();
    System.out.printf("Size of current store is %d\n", length);
    for (int i = 0; i < readCnt; i++) {
      int keyId = rand.nextInt(_initialCapacity);
      String str = "key." + keyId;
      //byte[] key = str.getBytes();
      //byte[] value = _store.get(key);
      tmp = _store.get(str);
      System.out.printf("Key=%s\tValue=%s%n", str, tmp.getVarName());
   }
  }

  @Override
  public boolean isOpen() {
    return _store.isOpen();
  }

  @Override
  public void open() throws IOException {
    _store.open();
  }

  public void close() throws IOException {
    _store.close();
  }

  public static void main(String[] args) {
    try {
      File homeDir = new File(args[0]);
      int initialCapacity = Integer.parseInt(args[1]);
      KratiDataStore store = new KratiDataStore(homeDir, initialCapacity);
      //store.populate();
      store.doRandomReads(10);
      store.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
