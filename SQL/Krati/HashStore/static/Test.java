import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import krati.store.SerializableObjectStore;

public class Test {

  public static HashStore mValueStore = null;
  public static HashStore mPathStore = null;

  /**
   * Select the satisfied values from the Hash Store.
   * @param keylist the selected keys
   * @param valuelist the query condition value
   * @param typelist the query type(e.g. equal|lt|st)
   */
  private static Set<String> selectValues(List<String> keylist, List<String> valuelist, List<String> typelist) {
    Set<String> ret = new HashSet();
    Set<String> keyset = new HashSet();
    Set<String> tmpvalueset = new HashSet();
    boolean hasRepeatedKey = false;
    try {
      //for each key in the keylist
      for (int i = 0; i < keylist.size(); i++) {        
        String tmpkey = keylist.get(i);
        if (!keyset.add(tmpkey)) {
          hasRepeatedKey = true;
        }
        Set<String> valueset = mValueStore.get(tmpkey);
        Iterator<String> iterator = valueset.iterator();
        if (valueset == null)
          continue;
        String currentvalue = valuelist.get(i);
        String selecttype = typelist.get(i);
        switch (selecttype) {
          case "eq" : {
            if (valueset.contains(currentvalue)) {
              ret.add(tmpkey.concat(currentvalue));
            }
            break;
          }
          case "lt" : {
            if (!isNumeric(currentvalue)) {
              break;
            }
            double cvalue = Double.valueOf(currentvalue);
            while (iterator.hasNext()) {
              String tmpvalue = iterator.next();
              if (isNumeric(tmpvalue)) {
                double ivalue = Double.valueOf(tmpvalue);
                if (ivalue >= cvalue) {
                  if (!hasRepeatedKey) {
                    tmpvalueset.add(tmpkey.concat(tmpvalue));
                  } else {
                    if(tmpvalueset.contains(tmpkey.concat(tmpvalue))) {
                      ret.add(tmpkey.concat(tmpvalue));
                    }
                  }
                }
              } else {
                continue;
              }
            }
            break;
          }
          case "st" : {
            if (!isNumeric(currentvalue)) {
              break;
            }
            double cvalue = Double.valueOf(currentvalue);
            while (iterator.hasNext()) {
              String tmpvalue =  iterator.next();
              if (isNumeric(tmpvalue)) {
                double ivalue = Double.valueOf(tmpvalue);
                if (ivalue <= cvalue) {
                  if (!hasRepeatedKey) {
                    tmpvalueset.add(tmpkey.concat(tmpvalue));
                  } else {
                    if(tmpvalueset.contains(tmpkey.concat(tmpvalue))) {
                      ret.add(tmpkey.concat(tmpvalue));
                    }
                  }
                }
              } else {
                continue;
              }
            }
            break;
          }
          default :
            System.out.println("Select type error");
        } //End switch
      } // End for
      if (!hasRepeatedKey) { //only lt or st seceltion
        ret.addAll(tmpvalueset);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    keyset = null;
    tmpvalueset = null;
    return ret;
  }

  /**
   * Whether the input String is numeric
   * @param str the input String
   */
  private static boolean isNumeric(String str) {
    int i;
    for (i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Retrieve the path list of corresponding keys.
   * @param keylist the selected keys
   */
  private static Set<String> selectPaths(Set<String> keylist) {
    Set<String> ret = new HashSet();
    Iterator<String> iterator = keylist.iterator();
    while (iterator.hasNext()) {
      Set<String> pathset = mPathStore.get(iterator.next());
      if (pathset != null) {
        ret.addAll(pathset);
      }
    }
    return ret;
  }

  public static void main(String[] args) {
    //Init the hash data base
    try {

      SerializableObjectStore<String, Set<String>> sPathStore = HashStore.PATHSTORE;
      if (sPathStore != null) {
        System.out.println("HashStore is opened");
      } else {
        System.out.println("HashStore is not opened");
        sPathStore = HashStore.staticDataStore();
      }
      mValueStore = new HashStore(new File("/home/condor/test/SQL/Krati/HashStore/ValueStore"), 10240);
      mPathStore = new HashStore(new File("/home/condor/test/SQL/Krati/HashStore/PathStore"), 10240);
      if ((mValueStore == null) || (mPathStore == null)) {
        System.out.println("Hash store init failed!");
        System.exit(-1);
      }
      mValueStore.populate(100, "key.", "000");
      for (int i = 0; i < 10; i++)
        mPathStore.populate(10, "key.10000", "path");
      mValueStore.sync();
      mPathStore.sync();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Populate data to HashStore");
    //Init query conditions
    List<String> keylist = new ArrayList();
    List<String> valuelist = new ArrayList();
    List<String> typelist = new ArrayList();
    keylist.add("key.10");
    valuelist.add("0001");
    typelist.add("eq");
    //Retrieve key list
    Set<String> pathkey = selectValues(keylist, valuelist, typelist);
    if (pathkey == null) {
      System.out.println("No satisfied KV pairs in Hash Store");
      System.exit(-1);
    } else {
      Iterator<String> iterator = pathkey.iterator();
      while (iterator.hasNext()) {
        System.out.println("Selected key : " + iterator.next());
      }
    }
    //Retrieve path list
    Set<String> pathlist = selectPaths(pathkey);
    if (pathlist == null) {
      System.out.println("No satisfied KV pairs in Hash Store");
      System.exit(-1);
    } else {
      Iterator<String> iterator = pathlist.iterator();
      while (iterator.hasNext()) {
        System.out.println("Path : " + iterator.next());
      }
    }
  }
}
