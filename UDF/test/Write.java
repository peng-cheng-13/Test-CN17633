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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import alluxio.util.ConfigurationUtils;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

public class Write{

  static int dirfiles = 100;
  static String initdir = "/udmtest";
  static int depth;
  static FileSystem fs;

private static void createdir(int currentdepth, String currentdir) throws Exception {
  int nextdepth = currentdepth + 1;
  System.out.println("Currentdepth is" + currentdepth);
  System.out.println("Currentdir is" + currentdir);
  if (currentdepth < depth) {
    for (int i = 0; i < dirfiles; i++) {
      String nextpath = currentdir + "/" + i;
      AlluxioURI tmppath  = new AlluxioURI(nextpath);
      fs.createDirectory(tmppath);
      createdir(nextdepth, nextpath);
    }
  } else {
    for (int i = 0; i < dirfiles; i++) {
      String nextpath = currentdir + "/file-" + i;
      AlluxioURI tmppath  = new AlluxioURI(nextpath);
      fs.createFile(tmppath);
    }
  }
}


public static void main(String[] args) {
  depth = Integer.parseInt(args[0]);
  try {
    String confile = "/home/condor/alluxio/conf/alluxio-site.properties";
    Properties mypro = ConfigurationUtils.loadPropertiesFromFile(confile);
    Configuration.merge(mypro);
    CreateFileOptions options = CreateFileOptions.defaults();
    fs = FileSystem.Factory.get();
    AlluxioURI rootpath = new AlluxioURI(initdir);
    fs.createDirectory(rootpath);
    createdir(1, initdir);
  } catch (Exception e) {
    System.out.println(e);
  }

}

}
