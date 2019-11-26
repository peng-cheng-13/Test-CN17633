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

private static int b_to_int(byte[] b,int offset){
  int i = 0;
  int ret = 0;
  for(i=0;i<4;i++){
    ret += (b[offset+4-i-1] & 0xFF) << (i*8);
  }
  return ret;
}

private static long b_to_long(byte[] b,int offset){
  int i = 0;
  long ret = 0;
  for(i=0;i<8;i++){
    ret += (long)(b[offset+8-i-1] & 0xFF) << (long)(i*8);
  }
  return ret;
}

private static double b_to_double(byte[] b,int offset){
  int i = 0;
  long ret = 0;
  for(i=0;i<8;i++){
    ret |= ((long)(b[offset+8-i-1] & 0xFF)) << (i*8);
  }
  System.out.printf("ret is %d\n",ret);
  return Double.longBitsToDouble(ret);
}

private static float b_to_float(byte[] b,int offset){
  int i = 0;
  int ret = 0;
  for(i=0;i<4;i++){
    ret |= (b[offset+4-i-1] & 0xFF) << (i*8);
  }
  System.out.printf("ret is %d\n",ret);
  return Float.intBitsToFloat(ret);
}

private static short b_to_short(byte[] b,int offset){
  int i = 0;
  short ret = 0;
  for(i=0;i<2;i++){
    ret |= (b[offset+2-i-1] & 0xFF) << (i*8);
  }
  return ret;
}

private static byte[] double2byte(double[] a){
  byte[] b = new byte[a.length * 8];
  for (int i = 0; i < a.length; i++){
    long dbit = Double.doubleToLongBits(a[i]);
    for (int j = 0; j < 8; j++) {
      //Big-endian* b[i*8+j] = (byte) (dbit >> (56 - j * 8));
      b[i*8+j] = (byte) (dbit >> (j * 8));
    }
  }
  return b;
}

private static byte[] float2byte(float[] f){
  byte[] b = new byte[f.length * 4];
  for (int i = 0; i < f.length; i++){
    int fbit = Float.floatToIntBits(f[i]);
    for (int j = 0; j < 4; j++) {  
          //Big-endian* b[i*4+j] = (byte) (fbit >> (24 - j * 8)); 
          b[i*4+j] = (byte) (fbit >> (j * 8));
    }
  }
  /*
  int len = b.length;
  byte[] dest = new byte[len];
  System.arraycopy(b, 0, dest, 0, len);
  byte temp;
  for (int i = 0; i < len / 2; ++i) {
	temp = dest[i];
	dest[i] = dest[len - i - 1];
	dest[len - i - 1] = temp;
  }
  return dest;
  */
  return b;
}

private static byte[] int2byte(int[] a){
  byte[] ret = new byte[a.length * 4];
  for (int i = 0; i < a.length; i++){
      for(int j =0;j<4;j++)
          //Big-endian* ret[i*4+j] = (byte) (a[i] >> (24 - j * 8));
          ret[i*4+j] = (byte) (a[i] >> (j * 8));
  }
  return ret;
}

private static byte[] long2byte(long[] a){
  byte[] ret = new byte[a.length * 8];
  for (int i = 0; i < a.length; i++){
      for(int j =0;j<8;j++)
          //Big-endian* ret[i*8+j] = (byte) (a[i] >> (56 - j * 8));
          ret[i*8+j] = (byte) (a[i] >> (j * 8));
  }
  return ret;
}

public static void main(String[] arg){

	try{
		String confile = "/home/condor/alluxio/conf/alluxio-site.properties";
        	Properties mypro = ConfigurationUtils.loadPropertiesFromFile(confile);
        	Configuration.merge(mypro);
		CreateFileOptions options = CreateFileOptions.defaults();
		//options = options.setLocationPolicy(new LocalFirstPolicy());
		ArrayList<String> var = new ArrayList<String>();
                ArrayList<String> type = new ArrayList<String>();
                var.add("test");
                var.add("id");
                //var.add("genid");
                type.add("DOUBLE");
                type.add("DOUBLE");
                //type.add("DOUBLE");
                options.setFileInfo(var, type);
                System.out.printf("My Block size is %d\n",options.getBlockSizeBytes());
		FileSystem fs = FileSystem.Factory.get();
	        AlluxioURI path = new AlluxioURI("/testfile-java");
		//byte[] b = new byte [131072];
		float bd =0 ;
		int i =0;
	        //for(i=0; i<131072;i++){
        	//        b[i]=6;
        	//}
        	byte c[] = {1,1};
                //float[] input = new float [1024];
                //for(i=0; i<1024;i++){
                //   input[i]=(float) i;
                //}
                double[] input = new double [8192];
                for(i=0; i<8192;i++){
                  input[i]=(double) i + 1;
                }
                //byte[] b = float2byte(input);
                byte[] b = double2byte(input);
                System.out.println(b_to_float(b,4));
                System.out.println((double)input[0]);
        	FileOutStream out = fs.createFile(path,options);
                out.buildIndex(4);
        	System.out.println("Alluxio has created");
        	long startTime = System.currentTimeMillis();
        	for(i=0; i<8192*16;i++){ 
                     	out.write(b);
        	}
        	long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
                if(out.shouldIndex()){
                  System.out.printf("Block max value is %f \n",out.getBlockMax());
                  System.out.printf("Block min value is %f \n",out.getBlockMin());
                } else {
                   System.out.println("Error in generate index");
                }
		out.close();
		bd = 16*1024*1000/(endTime - startTime);
		System.out.printf("Write Bandwidth = %f MB/s \n",bd);
	        System.out.println("Output finished");

                byte[] test = new byte[8];
                test[0] = (byte)0x40;
                test[1] = (byte)0x7f;
                test[2] = (byte)0x73;
                test[3] = (byte)0x6a;
                test[4] = (byte)0x11;
                test[5] = (byte)0x00;
		test[6] = (byte)0x00;
		test[7] = (byte)0x01;
                int a = b_to_int(test,0);
                long lvalue = b_to_long(test,0);
                double dvalue = b_to_double(test,0);
                float[] f0 = {12.34f};
		System.out.println(f0[0]);
		byte[] f1 = float2byte(f0);
		float fvalue = b_to_float(f1,0); 
                short svalue = b_to_short(test,6);
                System.out.printf("Turn byte[] to int with return value %d \n",a);
		System.out.printf("Turn byte[] to long with return value %d \n",lvalue);
                System.out.printf("Turn byte[] to double with return value %f \n",dvalue);
                System.out.printf("Turn byte[] to float with return value %f \n",fvalue);
                System.out.printf("Turn byte[] to short with return value %d \n",svalue);
		
	}catch(Exception e){
	  System.out.println(e);
        }

}

}
