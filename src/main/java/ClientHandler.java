import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private final Socket clientSocket;
    private final RedisStore redisStore;

    public ClientHandler(Socket clientSocket, RedisStore redisStore) {
        this.clientSocket = clientSocket;
        this.redisStore = redisStore;
    }

    @Override
    public void run() {
        try (
                Socket socket = clientSocket;
                InputStream input = socket.getInputStream();
                OutputStream output = socket.getOutputStream()
        ) {
            RespParser parser = new RespParser(input);
            CommandHandler commandHandler = new CommandHandler(redisStore);

            while (true) {
                var command = parser.readCommand();

                // Client closed the connection.
                if (command == null) {
                    break;
                }

                byte[] response = commandHandler.handle(command);

                output.write(response);
                output.flush();
            }

        } catch (IOException e) {
            System.err.println("Client connection ended: " + e.getMessage());
        }
    }
}
