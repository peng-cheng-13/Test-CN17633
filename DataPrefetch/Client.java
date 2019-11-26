import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {
  public static void main(String[] args) {
    try {
      Socket socket = new Socket("localhost", 8889);
      OutputStream os = socket.getOutputStream();
      PrintWriter pw = new PrintWriter(os);
      String v1 = "11111111";
      String v2 = "22222";
      pw.write(v1 + "\n");
      pw.flush();
      pw.write(v2);
      pw.flush();
      socket.shutdownInput();
      pw.close();
      os.close();
      socket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
