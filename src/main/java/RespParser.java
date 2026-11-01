import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    private final InputStream input;

    public RespParser(InputStream input) {
        this.input = input;
    }

    public List<String> readCommand() throws IOException {
        String header = readLine();

        if (header == null) {
            return null;
        }

        if (!header.startsWith("*")) {
            throw new IOException("Expected RESP array");
        }

        int argumentCount;

        try {
            argumentCount = Integer.parseInt(header.substring(1));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid RESP array length");
        }

        if (argumentCount < 1) {
            throw new IOException("Invalid command argument count");
        }

        List<String> command = new ArrayList<>(argumentCount);

        for (int i = 0; i < argumentCount; i++) {
            String bulkHeader = readLine();

            if (bulkHeader == null || !bulkHeader.startsWith("$")) {
                throw new IOException("Expected RESP bulk string");
            }

            int length;

            try {
                length = Integer.parseInt(bulkHeader.substring(1));
            } catch (NumberFormatException e) {
                throw new IOException("Invalid bulk string length");
            }

            if (length < 0) {
                throw new IOException("Null or invalid bulk string unsupported");
            }

            byte[] data = readExactly(length);
            expectCRLF();

            command.add(new String(data, StandardCharsets.UTF_8));
        }

        return command;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        while (true) {
            int current = input.read();

            if (current == -1) {
                if (buffer.size() == 0) {
                    return null;
                }
                throw new EOFException("Unexpected end of RESP line");
            }

            if (current == '\r') {
                int next = input.read();

                if (next != '\n') {
                    throw new IOException("Expected CRLF");
                }

                return buffer.toString(StandardCharsets.UTF_8);
            }

            if (current == '\n') {
                throw new IOException("Expected CRLF");
            }

            buffer.write(current);
        }
    }

    private byte[] readExactly(int length) throws IOException {
        byte[] data = new byte[length];
        int offset = 0;

        while (offset < length) {
            int bytesRead = input.read(
                    data, offset, length - offset
            );

            if (bytesRead == -1) {
                throw new EOFException("Incomplete RESP bulk string");
            }

            offset += bytesRead;
        }

        return data;
    }

    private void expectCRLF() throws IOException {
        int cr = input.read();
        int lf = input.read();

        if (cr != '\r' || lf != '\n') {
            throw new IOException("Expected CRLF after bulk string");
        }
    }
}
