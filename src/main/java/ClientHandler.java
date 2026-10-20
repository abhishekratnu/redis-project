import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ClientHandler {
    public static void handleClient(Socket clientSocket) {
        try {
            InputStream inputStream = clientSocket.getInputStream();
            byte[] buffer = new byte[1024];
            int byteRead;
            while((byteRead = inputStream.read(buffer)) != -1) {
                clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
