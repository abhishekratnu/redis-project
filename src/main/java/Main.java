import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args) throws IOException {
      ClientHandler client = new ClientHandler();
      ExecutorService pool = Executors.newFixedThreadPool(4);
      try(ServerSocket serverSocket = new ServerSocket(6379)) {
          System.out.println("Redis Server listening");

          while(true) {
              Socket clientSocket = serverSocket.accept();
              pool.submit(() -> ClientHandler.handleClient(clientSocket));
          }
      } finally {
          pool.shutdown();
      }
  }


}
