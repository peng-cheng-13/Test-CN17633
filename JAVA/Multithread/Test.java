import java.util.concurrent.*;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

public class Test {

  public static void main(String[] args) throws ExecutionException,
      InterruptedException{
    System.out.println("Start");
    Date date1 = new Date();
    int taskSize = 5;
    ExecutorService pool = Executors.newFixedThreadPool(taskSize);
    List<Future> list = new ArrayList<Future>();
    for (int i = 0; i < taskSize; i++) {
      Callable c = new MyCallable(i + " ");
      Future f = pool.submit(c);
      list.add(f);
    }
    pool.shutdown();
    for (Future f : list) {
      System.out.println(">>>" + f.get().toString());
    }

    Date date2 = new Date();
    System.out.println("Execution time is : " + (date2.getTime() - date1.getTime()) + "ms");
  }
}

class MyCallable implements Callable<Object> {
  private String taskNum;
 
  MyCallable(String taskNum) {
    this.taskNum = taskNum;
  }
  
  public Object call() throws Exception {
    System.out.println(">>>" + taskNum + "task start");
    Date dateTmp1 = new Date();
    Thread.sleep(1000);
    Date dateTmp2 = new Date();
    long time = dateTmp2.getTime() - dateTmp1.getTime();
    return taskNum + "End in " + time + "ms";
  }
}
