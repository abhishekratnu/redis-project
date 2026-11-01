
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RedisStore {

    private static class Entry {
        private final Object value;
        private final long expiresAtMillis;

        Entry(Object value, long expiresAtMillis) {
            this.value = value;
            this.expiresAtMillis = expiresAtMillis;
        }
    }

    private final ConcurrentMap<String, Entry> data =
            new ConcurrentHashMap<>();

    private final Map<String, Long> keyVersions = new HashMap<>();

    private long globalVersion = 0;

    public synchronized void set(String key, String value) {
        data.put(key, new Entry(value, 0));
        markModified(key);
    }

    public synchronized void set(
            String key, String value, long ttlMillis) {
        long expiresAt = System.currentTimeMillis() + ttlMillis;
        data.put(key, new Entry(value, expiresAt));
        markModified(key);
    }

    public synchronized String get(String key) {
        Entry entry = getValidEntry(key);

        if (entry == null) {
            return null;
        }

        if (!(entry.value instanceof String)) {
            throw new IllegalStateException("WRONGTYPE");
        }

        return (String) entry.value;
    }

    public synchronized long rpush(
            String key, List<String> values) {
        Entry entry = getValidEntry(key);
        List<String> list;

        if (entry == null) {
            list = new ArrayList<>();
            data.put(key, new Entry(list, 0));
        } else if (entry.value instanceof List<?>) {
            list = getList(entry);
        } else {
            throw new IllegalStateException("WRONGTYPE");
        }

        list.addAll(values);
        notifyAll();
        return list.size();
    }

    public synchronized long lpush(
            String key, List<String> values) {
        Entry entry = getValidEntry(key);
        List<String> list;

        if (entry == null) {
            list = new ArrayList<>();
            data.put(key, new Entry(list, 0));
        } else if (entry.value instanceof List<?>) {
            list = getList(entry);
        } else {
            throw new IllegalStateException("WRONGTYPE");
        }

        // Each value is inserted at the left, in command order.
        for (String value : values) {
            list.add(0, value);
        }
        notifyAll();
        return list.size();
    }

    public synchronized List<String> lrange(
            String key, long start, long stop) {
        Entry entry = getValidEntry(key);

        if (entry == null) {
            return new ArrayList<>();
        }

        List<String> list = requireList(entry);
        int size = list.size();

        // Redis supports negative indexes from the end.
        long from = start < 0 ? size + start : start;
        long to = stop < 0 ? size + stop : stop;

        from = Math.max(0, from);
        to = Math.min(size - 1L, to);

        if (from >= size || to < 0 || from > to) {
            return new ArrayList<>();
        }

        return new ArrayList<>(
                list.subList((int) from, (int) to + 1)
        );
    }

    public synchronized String lpop(String key) {
        Entry entry = getValidEntry(key);

        if (entry == null) {
            return null;
        }

        List<String> list = requireList(entry);

        if (list.isEmpty()) {
            return null;
        }

        String result = list.remove(0);
        removeIfEmpty(key, entry, list);
        return result;
    }

    public synchronized String rpop(String key) {
        Entry entry = getValidEntry(key);

        if (entry == null) {
            return null;
        }

        List<String> list = requireList(entry);

        if (list.isEmpty()) {
            return null;
        }

        String result = list.remove(list.size() - 1);
        removeIfEmpty(key, entry, list);
        return result;
    }

    public synchronized List<String> lpop(
            String key, int count) {
        return popCount(key, count, true);
    }

    public synchronized List<String> rpop(
            String key, int count) {
        return popCount(key, count, false);
    }

    private List<String> popCount(
            String key, int count, boolean fromLeft) {
        Entry entry = getValidEntry(key);
        List<String> result = new ArrayList<>();

        if (entry == null) {
            return result;
        }

        List<String> list = requireList(entry);
        int limit = Math.min(count, list.size());

        for (int i = 0; i < limit; i++) {
            if (fromLeft) {
                result.add(list.remove(0));
            } else {
                result.add(list.remove(list.size() - 1));
            }
        }

        removeIfEmpty(key, entry, list);
        return result;
    }

    public synchronized long llen(String key) {
        Entry entry = getValidEntry(key);

        if (entry == null) {
            return 0;
        }

        return requireList(entry).size();
    }

    private List<String> getList(Entry entry) {
        @SuppressWarnings("unchecked")
        List<String> list = (List<String>) entry.value;
        return list;
    }

    private List<String> requireList(Entry entry) {
        if (!(entry.value instanceof List<?>)) {
            throw new IllegalStateException("WRONGTYPE");
        }

        return getList(entry);
    }

    private void removeIfEmpty(
            String key, Entry entry, List<String> list) {
        if (list.isEmpty()) {
            data.remove(key, entry);
        }
    }

    private Entry getValidEntry(String key) {
        Entry entry = data.get(key);

        if (entry != null
                && entry.expiresAtMillis != 0
                && System.currentTimeMillis()
                >= entry.expiresAtMillis) {

            if (data.remove(key, entry)) {
                markModified(key);
            }

            return null;
        }

        return entry;
    }


    public synchronized List<String> blpop(
            List<String> keys, long timeoutMillis)
            throws InterruptedException {

        long deadline = System.nanoTime()
                + timeoutMillis * 1_000_000L;

        while (true) {
            // Check keys in the order supplied by the client.
            for (String key : keys) {
                Entry entry = getValidEntry(key);

                if (entry == null) {
                    continue;
                }

                List<String> list = requireList(entry);

                if (!list.isEmpty()) {
                    String value = list.remove(0);
                    removeIfEmpty(key, entry, list);

                    List<String> result = new ArrayList<>();
                    result.add(key);
                    result.add(value);

                    return result;
                }
            }

            // A timeout of zero means wait indefinitely.
            if (timeoutMillis == 0) {
                wait();
                continue;
            }

            long remainingNanos = deadline - System.nanoTime();

            if (remainingNanos <= 0) {
                return null;
            }

            long millis = remainingNanos / 1_000_000L;
            int nanos = (int) (remainingNanos % 1_000_000L);

            wait(millis, nanos);
        }
    }


    public synchronized String type(String key) {
        Entry entry = getValidEntry(key);

        if (entry == null) {
            return "none";
        }

        if (entry.value instanceof String) {
            return "string";
        }

        if (entry.value instanceof List<?>) {
            return "list";
        }

        if (entry.value instanceof StreamData) {
            return "stream";
        }

        return "none";
    }


    private static class StreamEntry {
        private final String id;
        private final List<String> fields;

        StreamEntry(String id, List<String> fields) {
            this.id = id;
            this.fields = new ArrayList<>(fields);
        }
    }

    private static class StreamData {
        private final List<StreamEntry> entries = new ArrayList<>();
        private long lastMillis = 0;
        private long lastSequence = 0;
    }


    public synchronized String xadd(
            String key, String requestedId, List<String> fields) {

        Entry entry = getValidEntry(key);
        StreamData stream;

        if (entry == null) {
            stream = new StreamData();
            data.put(key, new Entry(stream, 0));
        } else if (entry.value instanceof StreamData) {
            stream = (StreamData) entry.value;
        } else {
            throw new IllegalStateException("WRONGTYPE");
        }

        long millis;
        long sequence;
        String id;


        if (requestedId.equals("*")) {
            long now = System.currentTimeMillis();

            if (now > stream.lastMillis) {
                millis = now;
                sequence = 0;
            } else {
                millis = stream.lastMillis;

                if (stream.lastSequence == Long.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            "stream sequence number overflow");
                }

                sequence = stream.lastSequence + 1;
            }

            id = millis + "-" + sequence;
        } else {
        // An explicit ID must contain exactly two non-negative
        // decimal integers separated by one hyphen.
        if (!requestedId.matches("[0-9]+-[0-9]+")) {
            throw new IllegalArgumentException(
                    "Invalid stream ID format");
        }

        String[] parts = requestedId.split("-", -1);

        try {
            millis = Long.parseLong(parts[0]);
            sequence = Long.parseLong(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Stream ID is out of range");
        }

        // Redis does not allow 0-0 as a new entry ID.
        if (millis == 0 && sequence == 0) {
            throw new IllegalArgumentException(
                    "The ID specified in XADD must be greater than 0-0");
        }

        // Every new ID must be strictly greater than the last ID.
        if (millis < stream.lastMillis
                || (millis == stream.lastMillis
                && sequence <= stream.lastSequence)) {
            throw new IllegalArgumentException(
                    "The ID specified in XADD is equal or smaller than the target stream top item");
        }

        id = requestedId;
    }

        stream.entries.add(new StreamEntry(id, fields));
        stream.lastMillis = millis;
        stream.lastSequence = sequence;

        notifyAll();

        return id;
    }


    public synchronized List<List<String>> xrange(
            String key, String startId, String endId) {

        Entry entry = getValidEntry(key);
        List<List<String>> result = new ArrayList<>();

        if (entry == null) {
            return result;
        }

        if (!(entry.value instanceof StreamData)) {
            throw new IllegalStateException("WRONGTYPE");
        }

        StreamData stream = (StreamData) entry.value;

        // "-" means the beginning of the stream.
        long[] start = startId.equals("-")
                ? new long[]{0, 0}
                : parseStreamId(startId);

        // "+" means the end of the stream.
        long[] end = endId.equals("+")
                ? new long[]{Long.MAX_VALUE, Long.MAX_VALUE}
                : parseStreamId(endId);

        if (compareIds(start, end) > 0) {
            return result;
        }

        for (StreamEntry streamEntry : stream.entries) {
            long[] currentId = parseStreamId(streamEntry.id);

            // Both boundaries are inclusive.
            if (compareIds(currentId, start) >= 0
                    && compareIds(currentId, end) <= 0) {

                List<String> row = new ArrayList<>();
                row.add(streamEntry.id);
                row.addAll(streamEntry.fields);

                result.add(row);
            }
        }

        return result;
    }

    private long[] parseStreamId(String id) {
        if (!id.matches("[0-9]+-[0-9]+")) {
            throw new IllegalArgumentException(
                    "Invalid stream ID format");
        }

        String[] parts = id.split("-", -1);

        try {
            long millis = Long.parseLong(parts[0]);
            long sequence = Long.parseLong(parts[1]);

            return new long[]{millis, sequence};
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Stream ID out of range");
        }
    }

    private int compareIds(long[] first, long[] second) {
        int millisComparison = Long.compare(
                first[0], second[0]);

        if (millisComparison != 0) {
            return millisComparison;
        }

        return Long.compare(first[1], second[1]);
    }

    public synchronized Map<String, List<List<String>>> xread(
            List<String> keys,
            List<String> requestedIds,
            long blockMillis
    ) throws InterruptedException {

        // Preserve the order in which the streams were requested.
        Map<String, List<List<String>>> result = new LinkedHashMap<>();

        // Resolve "$" ONCE, before waiting.
        // For a missing stream, use 0-0 so its first entry can be read.
        List<String> startIds = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String requestedId = requestedIds.get(i);

            if (requestedId.equals("$")) {
                Entry entry = getValidEntry(key);

                if (entry == null) {
                    startIds.add("0-0");
                } else if (entry.value instanceof StreamData) {
                    StreamData stream = (StreamData) entry.value;
                    startIds.add(
                            stream.lastMillis + "-" + stream.lastSequence
                    );
                } else {
                    throw new IllegalStateException("WRONGTYPE");
                }
            } else {
                // Validate explicit stream IDs.
                parseStreamId(requestedId);
                startIds.add(requestedId);
            }
        }

        long startNanos = System.nanoTime();

        while (true) {
            result.clear();

            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                String startId = startIds.get(i);

                Entry entry = getValidEntry(key);

                // A stream might not exist yet.
                if (entry == null) {
                    continue;
                }

                if (!(entry.value instanceof StreamData)) {
                    throw new IllegalStateException("WRONGTYPE");
                }

                StreamData stream = (StreamData) entry.value;
                List<List<String>> matchingEntries = new ArrayList<>();

                long[] start = parseStreamId(startId);

                for (StreamEntry streamEntry : stream.entries) {
                    long[] current = parseStreamId(streamEntry.id);

                    // XREAD uses an exclusive start ID.
                    if (compareIds(current, start) > 0) {
                        List<String> row = new ArrayList<>();
                        row.add(streamEntry.id);
                        row.addAll(streamEntry.fields);
                        matchingEntries.add(row);
                    }
                }

                if (!matchingEntries.isEmpty()) {
                    result.put(key, matchingEntries);
                }
            }

            // Return as soon as at least one stream has new entries.
            if (!result.isEmpty()) {
                return result;
            }

            // -1 means non-blocking; 0 means wait indefinitely.
            if (blockMillis < 0) {
                return result;
            }

            if (blockMillis > 0) {
                long elapsedMillis =
                        (System.nanoTime() - startNanos) / 1_000_000L;

                long remainingMillis = blockMillis - elapsedMillis;

                if (remainingMillis <= 0) {
                    return null; // Timed out.
                }

                // Releases this monitor while waiting.
                wait(remainingMillis);
            } else {
                wait();
            }
        }
    }

    public synchronized long incr(String key) {

        Entry entry = getValidEntry(key);

        // Key doesn't exist or has expired
        if (entry == null) {
            data.put(key, new Entry("1", 0));
            return 1;
        }

        // Ensure the value is a string
        if (!(entry.value instanceof String)) {
            throw new IllegalStateException("WRONGTYPE");
        }

        long current;

        try {
            current = Long.parseLong((String) entry.value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "value is not an integer or out of range"
            );
        }

        long updated;

        try {
            updated = Math.addExact(current, 1);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "value is not an integer or out of range"
            );
        }

        // Preserve existing TTL
        data.put(key, new Entry(
                String.valueOf(updated),
                entry.expiresAtMillis
        ));

        return updated;
    }

    private void markModified(String key) {
        globalVersion++;
        keyVersions.put(key, globalVersion);
    }

    public synchronized long getVersion(String key) {

        // Handle expiration before reading the version.
        getValidEntry(key);

        return keyVersions.getOrDefault(key, 0L);
    }

    public synchronized boolean hasWatchedKeyChanged(
            Map<String, Long> watchedKeys) {

        for (Map.Entry<String, Long> watched : watchedKeys.entrySet()) {

            String key = watched.getKey();
            long previousVersion = watched.getValue();

            // Detect expiration as a modification.
            getValidEntry(key);

            long currentVersion =
                    keyVersions.getOrDefault(key, 0L);

            if (currentVersion != previousVersion) {
                return true;
            }
        }

        return false;
    }
}
