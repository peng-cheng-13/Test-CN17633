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
import alluxio.exception.AlluxioException;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.io.Closer;

import alluxio.util.ConfigurationUtils;
import java.util.Properties;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
  public static void main(String[] args) {
    try {
      String confile = "/home/condor/alluxio/conf/alluxio-site.properties";
      Properties mypro = ConfigurationUtils.loadPropertiesFromFile(confile);
      Configuration.merge(mypro);
      FileSystem fs = FileSystem.Factory.get();
      Closer closer = Closer.create();
      byte[] buf = new byte[8 * Constants.MB];

      ServerSocket ss = new ServerSocket(8889);
      Socket socket = ss.accept();
      InputStream is = socket.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      String info = null;
      while (true) {
        while ((info = br.readLine()) != null) {
          System.out.println("Prefetch path is " + info);
          AlluxioURI path = new AlluxioURI(info);
          FileInStream in = closer.register(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE).disablePrefetch()));
          while (in.read(buf) != -1) {
          }
          System.out.println(info + " prefetched");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }/* finally {
      socket.shutdownInput();
      br.close();
      isr.close();
      is.close();
      socket.close();
      ss.close();
      closer.close();
    }*/
  }
}
