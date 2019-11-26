import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Test {

  public static ListStore mValueStore = null;
  public static ListStore mPathStore = null;

  /**
   * Select the satisfied values from the Hash Store.
   * @param keylist the selected keys
   * @param valuelist the query condition value
   * @param typelist the query type(e.g. equal|lt|st)
   */
  private static List<String> selectValues(List<String> keylist, List<String> valuelist, List<String> typelist) {
    List<String> ret = new ArrayList();
    List<String> keyset = new ArrayList();
    List<String> tmpvalueset = new ArrayList();
    boolean hasRepeatedKey = false;
    try {
      //for each key in the keylist
      for (int i = 0; i < keylist.size(); i++) {        
        String tmpkey = keylist.get(i);
        if (!keyset.add(tmpkey)) {
          hasRepeatedKey = true;
        }
        List<String> valueset = mValueStore.get(tmpkey);
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
  private static List<String> selectPaths(List<String> keylist) {
    List<String> ret = new ArrayList();
    Iterator<String> iterator = keylist.iterator();
    while (iterator.hasNext()) {
      List<String> pathset = mPathStore.get(iterator.next());
      if (pathset != null) {
        ret.addAll(pathset);
      }
    }
    return ret;
  }

  public static void main(String[] args) {
    //Init the hash data base
    try {
      mValueStore = new ListStore(new File("/home/condor/test/SQL/Krati/ListStore/ValueStore"), 10240);
      mPathStore = new ListStore(new File("/home/condor/test/SQL/Krati/ListStore/PathStore"), 10240);
      if (mPathStore.isOpen())
        System.out.println("PathStore is opened");
      else
        System.out.println("PathStore is not opened");
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
    System.out.println("Populate data to ListStore");
    //Init query conditions
    List<String> keylist = new ArrayList();
    List<String> valuelist = new ArrayList();
    List<String> typelist = new ArrayList();
    keylist.add("key.10");
    valuelist.add("0001");
    typelist.add("eq");
    //Retrieve key list
    List<String> pathkey = selectValues(keylist, valuelist, typelist);
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
    List<String> pathlist = selectPaths(pathkey);
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
