import java.util.*;

public class CommandHandler {
    private final RedisStore store;

    public CommandHandler(RedisStore store) {
        this.store = store;
    }

    private boolean inTransaction = false;

    private final List<List<String>> queuedCommands = new ArrayList<>();
    private final Map<String, Long> watchedKeys = new HashMap<>();

    private byte[] executeTransaction(List<String> args) {

        if (args.size() != 1) {
            return RespEncoder.error(
                    "wrong number of arguments for 'exec' command"
            );
        }

        if (!inTransaction) {
            return RespEncoder.error("EXEC without MULTI");
        }

        List<List<String>> commands =
                new ArrayList<>(queuedCommands);

        inTransaction = false;
        queuedCommands.clear();

        List<byte[]> responses = new ArrayList<>();

        synchronized (store) {

            // Check whether any watched key changed.
            if (store.hasWatchedKeyChanged(watchedKeys)) {

                watchedKeys.clear();

                // Null array means transaction aborted.
                return RespEncoder.nullArray();
            }

            // Transaction is valid. Clear WATCH state.
            watchedKeys.clear();

            // Execute all queued commands.
            for (List<String> queuedCommand : commands) {
                responses.add(handle(queuedCommand));
            }
        }

        return RespEncoder.arrayOfReplies(responses);
    }

    public byte[] handle(List<String> args) {
        if (args == null || args.isEmpty()) {
            return RespEncoder.error("empty command");
        }

        String command = args.get(0).toUpperCase(Locale.ROOT);
        // If transaction is active, queue commands.
        if (inTransaction
                && !command.equals("EXEC")
                && !command.equals("DISCARD")) {

            if (command.equals("MULTI")) {
                return RespEncoder.error(
                        "MULTI calls can not be nested"
                );
            }

            if (command.equals("WATCH")) {
                return RespEncoder.error(
                        "WATCH inside MULTI is not allowed"
                );
            }

            queuedCommands.add(new ArrayList<>(args));

            return RespEncoder.simpleString("QUEUED");
        }

        switch (command) {
            case "PING":
                if (args.size() == 1) {
                    return RespEncoder.simpleString("PONG");
                }

                if (args.size() == 2) {
                    return RespEncoder.bulkString(args.get(1));
                }

                return RespEncoder.error(
                        "wrong number of arguments for 'ping' command"
                );

            case "ECHO":
                if (args.size() != 2) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'echo' command"
                    );
                }

                return RespEncoder.bulkString(args.get(1));


            case "SET":
                if (args.size() != 3 && args.size() != 5) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'set' command"
                    );
                }

                String key = args.get(1);
                String value = args.get(2);

                if (args.size() == 3) {
                    store.set(key, value);
                    return RespEncoder.simpleString("OK");
                }

                String option = args.get(3).toUpperCase(Locale.ROOT);
                long ttl;
                long multiplier;

                if (option.equals("EX")) {
                    multiplier = 1000L;       // seconds to milliseconds
                } else if (option.equals("PX")) {
                    multiplier = 1L;          // already milliseconds
                } else {
                    return RespEncoder.error("unsupported SET option");
                }

                try {
                    ttl = Long.parseLong(args.get(4));
                } catch (NumberFormatException e) {
                    return RespEncoder.error("invalid expire time");
                }

                if (ttl <= 0 || ttl > Long.MAX_VALUE / multiplier) {
                    return RespEncoder.error("invalid expire time");
                }

                store.set(key, value, ttl * multiplier);
                return RespEncoder.simpleString("OK");

            case "GET":
                if (args.size() != 2) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'get' command"
                    );
                }

                try {
                    String storedValue = store.get(args.get(1));

                    if (storedValue == null) {
                        return RespEncoder.nullBulkString();
                    }

                    return RespEncoder.bulkString(storedValue);
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }


            case "RPUSH":
                if (args.size() < 3) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'rpush' command"
                    );
                }

                String listKey = args.get(1);
                List<String> elements = args.subList(2, args.size());

                try {
                    long length = store.rpush(listKey, elements);
                    return RespEncoder.integer(length);
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }


            case "LPUSH": {
                if (args.size() < 3) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'lpush' command"
                    );
                }

                String listKey1 = args.get(1);
                List<String> elements1 = args.subList(2, args.size());

                try {
                    return RespEncoder.integer(
                            store.lpush(listKey1, elements1)
                    );
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }
            }

            case "LRANGE": {
                if (args.size() != 4) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'lrange' command"
                    );
                }

                long start;
                long stop;

                try {
                    start = Long.parseLong(args.get(2));
                    stop = Long.parseLong(args.get(3));
                } catch (NumberFormatException e) {
                    return RespEncoder.error("value is not an integer or out of range");
                }

                try {
                    return RespEncoder.arrayOfBulkStrings(
                            store.lrange(args.get(1), start, stop)
                    );
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }
            }

            case "LPOP": {
                if (args.size() != 2 && args.size() != 3) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'lpop' command"
                    );
                }

                try {
                    if (args.size() == 2) {
                        String popped = store.lpop(args.get(1));

                        return popped == null
                                ? RespEncoder.nullBulkString()
                                : RespEncoder.bulkString(popped);
                    }

                    int count = Integer.parseInt(args.get(2));

                    if (count < 0) {
                        return RespEncoder.error("value is out of range");
                    }

                    return RespEncoder.arrayOfBulkStrings(
                            store.lpop(args.get(1), count)
                    );
                } catch (NumberFormatException e) {
                    return RespEncoder.error("value is not an integer or out of range");
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }
            }

            case "RPOP": {
                if (args.size() != 2 && args.size() != 3) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'rpop' command"
                    );
                }

                try {
                    if (args.size() == 2) {
                        String popped = store.rpop(args.get(1));

                        return popped == null
                                ? RespEncoder.nullBulkString()
                                : RespEncoder.bulkString(popped);
                    }

                    int count = Integer.parseInt(args.get(2));

                    if (count < 0) {
                        return RespEncoder.error("value is out of range");
                    }

                    return RespEncoder.arrayOfBulkStrings(
                            store.rpop(args.get(1), count)
                    );
                } catch (NumberFormatException e) {
                    return RespEncoder.error("value is not an integer or out of range");
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }
            }

            case "LLEN": {
                if (args.size() != 2) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'llen' command"
                    );
                }

                try {
                    return RespEncoder.integer(store.llen(args.get(1)));
                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }
            }


            case "BLPOP": {
                if (args.size() < 3) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'blpop' command"
                    );
                }

                long timeoutSeconds;

                try {
                    timeoutSeconds = Long.parseLong(
                            args.get(args.size() - 1)
                    );
                } catch (NumberFormatException e) {
                    return RespEncoder.error(
                            "timeout is not an integer or out of range"
                    );
                }

                if (timeoutSeconds < 0
                        || timeoutSeconds > Long.MAX_VALUE / 1000L) {
                    return RespEncoder.error("timeout is out of range");
                }

                List<String> keys = args.subList(1, args.size() - 1);
                long timeoutMillis = timeoutSeconds * 1000L;

                try {
                    List<String> result = store.blpop(keys, timeoutMillis);

                    if (result == null) {
                        return RespEncoder.nullArray();
                    }

                    return RespEncoder.arrayOfBulkStrings(result);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return RespEncoder.error("blocking command interrupted");

                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                }
            }

            case "TYPE": {
                if (args.size() != 2) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'type' command"
                    );
                }

                String keyType = store.type(args.get(1));

                return RespEncoder.simpleString(keyType);
            }


            case "XADD": {
                if (args.size() < 5 || (args.size() - 3) % 2 != 0) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'xadd' command"
                    );
                }

                String streamKey = args.get(1);
                String requestedId = args.get(2);
                List<String> fields = args.subList(3, args.size());

                try {
                    String generatedId = store.xadd(
                            streamKey, requestedId, fields
                    );

                    return RespEncoder.bulkString(generatedId);

                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                } catch (IllegalArgumentException e) {
                    return RespEncoder.error(e.getMessage());
                }
            }


            case "XRANGE": {
                if (args.size() != 4) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'xrange' command"
                    );
                }

                try {
                    List<List<String>> entries = store.xrange(
                            args.get(1),
                            args.get(2),
                            args.get(3)
                    );

                    return RespEncoder.streamEntries(entries);

                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                } catch (IllegalArgumentException e) {
                    return RespEncoder.error(e.getMessage());
                }
            }

            case "XREAD": {
                if (args.size() < 4) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'xread' command"
                    );
                }

                int index = 1;
                long blockMillis = -1; // Default: non-blocking.

                // Optional BLOCK timeout.
                if (args.get(index).equalsIgnoreCase("BLOCK")) {
                    if (args.size() < 5) {
                        return RespEncoder.error(
                                "syntax error"
                        );
                    }

                    try {
                        blockMillis = Long.parseLong(args.get(index + 1));

                        if (blockMillis < 0) {
                            return RespEncoder.error(
                                    "timeout is negative"
                            );
                        }
                    } catch (NumberFormatException e) {
                        return RespEncoder.error(
                                "timeout is not an integer or out of range"
                        );
                    }

                    index += 2;
                }

                // STREAMS must follow the optional BLOCK argument.
                if (index >= args.size()
                        || !args.get(index).equalsIgnoreCase("STREAMS")) {
                    return RespEncoder.error("syntax error");
                }

                index++;

                int remaining = args.size() - index;

                // There must be at least one key and one ID.
                // The number of keys must equal the number of IDs.
                if (remaining < 2 || remaining % 2 != 0) {
                    return RespEncoder.error("syntax error");
                }

                int streamCount = remaining / 2;

                List<String> keys = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < streamCount; i++) {
                    keys.add(args.get(index + i));
                    ids.add(args.get(index + streamCount + i));
                }

                try {
                    Map<String, List<List<String>>> result =
                            store.xread(keys, ids, blockMillis);

                    // null means a blocking read timed out.
                    if (result == null) {
                        return RespEncoder.nullArray();
                    }

                    return RespEncoder.xreadStreams(result);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return RespEncoder.error("XREAD interrupted");

                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );

                } catch (IllegalArgumentException e) {
                    return RespEncoder.error(e.getMessage());
                }
            }

            case "INCR": {
                if (args.size() != 2) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'incr' command"
                    );
                }

                try {
                    return RespEncoder.integer(store.incr(args.get(1)));

                } catch (IllegalStateException e) {
                    return RespEncoder.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                    );
                } catch (IllegalArgumentException e) {
                    return RespEncoder.error(
                            "value is not an integer or out of range"
                    );
                }
            }

            case "MULTI": {
                if (args.size() != 1) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'multi' command"
                    );
                }

                inTransaction = true;
                queuedCommands.clear();

                return RespEncoder.simpleString("OK");
            }

            case "EXEC": {
                return executeTransaction(args);
            }

            case "DISCARD": {

                if (!inTransaction) {
                    return RespEncoder.error("DISCARD without MULTI");
                }

                inTransaction = false;
                queuedCommands.clear();
                watchedKeys.clear();

                return RespEncoder.simpleString("OK");
            }

            case "WATCH": {

                if (inTransaction) {
                    return RespEncoder.error(
                            "WATCH inside MULTI is not allowed"
                    );
                }

                if (args.size() < 2) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'watch' command"
                    );
                }

                for (int i = 1; i < args.size(); i++) {

                    String watchKey = args.get(i);

                    // Don't reset the original version if already watched.
                    watchedKeys.putIfAbsent(
                            watchKey,
                            store.getVersion(watchKey)
                    );
                }

                return RespEncoder.simpleString("OK");
            }

            case "UNWATCH": {

                if (args.size() != 1) {
                    return RespEncoder.error(
                            "wrong number of arguments for 'unwatch' command"
                    );
                }

                watchedKeys.clear();

                return RespEncoder.simpleString("OK");
            }

            default:
                return RespEncoder.error("unknown command");

        }
    }
}
