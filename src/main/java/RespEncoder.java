import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespEncoder {

    private static final byte[] CRLF = {'\r', '\n'};

    public static byte[] simpleString(String value) {
        return ("+" + value + "\r\n")
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] bulkString(String value) {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        output.writeBytes(
                ("$" + data.length + "\r\n")
                        .getBytes(StandardCharsets.US_ASCII)
        );

        output.writeBytes(data);
        output.writeBytes(CRLF);

        return output.toByteArray();
    }

    public static byte[] error(String message) {
        return ("-ERR " + message + "\r\n")
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] nullBulkString() {
        return "$-1\r\n".getBytes(
                java.nio.charset.StandardCharsets.US_ASCII
        );
    }

    public static byte[] integer(long value) {
        return (":" + value + "\r\n").getBytes(
                java.nio.charset.StandardCharsets.US_ASCII
        );
    }


    public static byte[] arrayOfBulkStrings(
            java.util.List<String> values) {

        StringBuilder response = new StringBuilder();
        response.append("*").append(values.size()).append("\r\n");

        for (String value : values) {
            response.append("$")
                    .append(value.getBytes(
                            java.nio.charset.StandardCharsets.UTF_8
                    ).length)
                    .append("\r\n")
                    .append(value)
                    .append("\r\n");
        }

        return response.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8
        );
    }

    public static byte[] nullArray() {
        return "*-1\r\n".getBytes(
                java.nio.charset.StandardCharsets.US_ASCII
        );
    }


    public static byte[] streamEntries(
            java.util.List<java.util.List<String>> entries) {

        StringBuilder response = new StringBuilder();

        // Outer array: number of stream entries.
        response.append("*")
                .append(entries.size())
                .append("\r\n");

        for (java.util.List<String> entry : entries) {

            // Each entry contains two elements:
            // 1. The stream ID
            // 2. An array of field-value pairs.
            response.append("*2\r\n");

            appendBulkString(response, entry.get(0));

            int fieldCount = entry.size() - 1;

            response.append("*")
                    .append(fieldCount)
                    .append("\r\n");

            for (int i = 1; i < entry.size(); i++) {
                appendBulkString(response, entry.get(i));
            }
        }

        return response.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8
        );
    }

    private static void appendBulkString(
            StringBuilder response, String value) {

        int byteLength = value.getBytes(
                java.nio.charset.StandardCharsets.UTF_8
        ).length;

        response.append("$")
                .append(byteLength)
                .append("\r\n")
                .append(value)
                .append("\r\n");
    }

    public static byte[] xreadStreams(
            java.util.Map<String, java.util.List<java.util.List<String>>> streams
    ) {
        StringBuilder response = new StringBuilder();

        // Array of streams returned.
        response.append("*")
                .append(streams.size())
                .append("\r\n");

        for (java.util.Map.Entry<
                String,
                java.util.List<java.util.List<String>>> stream
                : streams.entrySet()) {

            // Each stream result: [stream key, entries].
            response.append("*2\r\n");
            appendBulkString(response, stream.getKey());

            java.util.List<java.util.List<String>> entries =
                    stream.getValue();

            response.append("*")
                    .append(entries.size())
                    .append("\r\n");

            for (java.util.List<String> entry : entries) {
                // Each entry: [ID, [field, value, ...]].
                response.append("*2\r\n");
                appendBulkString(response, entry.get(0));

                int fieldCount = entry.size() - 1;
                response.append("*")
                        .append(fieldCount)
                        .append("\r\n");

                for (int i = 1; i < entry.size(); i++) {
                    appendBulkString(response, entry.get(i));
                }
            }
        }

        return response.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8
        );
    }

    public static byte[] arrayOfReplies(List<byte[]> replies) {

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // RESP array header
        output.writeBytes(
                ("*" + replies.size() + "\r\n")
                        .getBytes(StandardCharsets.US_ASCII)
        );

        // Append each already-encoded RESP response.
        for (byte[] reply : replies) {
            output.writeBytes(reply);
        }

        return output.toByteArray();
    }
}
