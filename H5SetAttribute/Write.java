import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import com.google.common.collect.ImmutableMap;
import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.*;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.exception.AlluxioException;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.wire.HDFDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import alluxio.util.ConfigurationUtils;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Write{

public static void main(String[] args) {
 
  try {
    String confile = "/home/condor/alluxio/conf/alluxio-site.properties";
    Properties mypro = ConfigurationUtils.loadPropertiesFromFile(confile);
    Configuration.merge(mypro);
    CreateFileOptions options = CreateFileOptions.defaults();
    FileSystem fs = FileSystem.Factory.get();
    AlluxioURI path = new AlluxioURI("/testfile-java");
    FileOutStream out = fs.createFile(path,options);
    out.close();
    SetAttributeOptions soptions = SetAttributeOptions.defaults();
    List<HDFDataSet> h5list = new ArrayList<>();
    Map<String, String> attr = new HashMap<>();
    attr.put("temperature", "40");
    for (int i = 0; i < 3; i++) {
      HDFDataSet tmp = new HDFDataSet("Dateset-"+i);
      tmp.setUDM(attr);
      h5list.add(tmp);
    }
    soptions.addH5(h5list);
    fs.setAttribute(path, soptions);
    System.out.println("Set Attribute Done");
  } catch (Exception e) {
    System.out.println(e);
  }

}

}
