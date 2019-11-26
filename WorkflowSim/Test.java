import java.lang.Runtime;
import java.lang.Process;
import java.io.BufferedReader;    
import java.io.IOException;    
import java.io.InputStream;    
import java.io.InputStreamReader;
//import ai.catboost.CatBoostModel;
//import ai.catboost.CatBoostPredictions;

public class Test {
  public static void main(String args[]) {
    try {
      Runtime rt = Runtime.getRuntime();
      long startTime = System.currentTimeMillis();
      Process proc = rt.exec("./Apply.sh inputdata");
      long endTime = System.currentTimeMillis();
      System.out.println("Fist call spend " + (endTime - startTime) + "ms");
      int exitVal = proc.waitFor();
      /*
      CatBoostModel model = CatBoostModel.loadModel("model.bin");
      float[] in1 = new float[5];
      in1[0] = 16f;
      in1[1] = 4222384.0f;
      in1[2] = 2f;
      in1[3] = 1f;
      in1[4] = 6f;
      String[] in2 = new String[1];
      in2[0] = "mProjectPP";
      CatBoostPredictions prediction = model.predict(in1, in2);
      System.out.println("model value is " + String.valueOf(prediction.get(0, 0)));
      */
      
      startTime = System.currentTimeMillis();
      proc = rt.exec("./CModel mConcatFit 1 338034.0 1 1 1");
      endTime = System.currentTimeMillis();
      System.out.println("Sceond call spend " + (endTime - startTime) + "ms");
      //exitVal = proc.waitFor();
      InputStream fis=proc.getInputStream();
      InputStreamReader isr=new InputStreamReader(fis);
      BufferedReader br=new BufferedReader(isr);
      String line=null;
      while((line=br.readLine())!=null) {
        System.out.println(line); 
      }
      System.out.println("Process exitValue: " + exitVal);
      
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
}
