import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) {

        int port = args.length > 0
                ? Integer.parseInt(args[0])
                : 6379;

        ExecutorService executor = Executors.newCachedThreadPool();

        RedisStore redisStore = new RedisStore();

        try (ServerSocket serverSocket = new ServerSocket()) {

            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));

            System.out.println("Redis server listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();

                System.out.println(
                        "Client connected: " +
                                clientSocket.getRemoteSocketAddress()
                );

                executor.submit(new ClientHandler(clientSocket, redisStore));
            }

        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}