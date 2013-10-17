package store.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import store.common.Event;
import static store.common.Event.event;
import store.common.Hash;
import store.common.Operation;
import store.common.Uid;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.io.ObjectDecoder;
import static store.server.io.ObjectEncoder.encoder;
import store.server.io.StreamDecoder;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;
import static store.server.transaction.TransactionManager.currentTransactionContext;

public class HistoryManager {

    private static final int PAGE_SIZE = 8192;
    private final AtomicLong nextId;
    private final Path root;
    private final Path latest;
    private final Path index;

    private HistoryManager(Path root) throws IOException {
        this.root = root;
        latest = root.resolve("latest");
        index = root.resolve("index");
        nextId = new AtomicLong(initNextId());
    }

    private long initNextId() throws IOException {
        Deque<Event> latestEvents = loadPage(latest);
        if (!latestEvents.isEmpty()) {
            return latestEvents.getLast().getId() + 1;
        }
        Deque<IndexEntry> indexEntries = loadIndex();
        if (!indexEntries.isEmpty()) {
            return indexEntries.getLast().last + 1;
        }
        return 1;
    }

    public static HistoryManager create(Path path) {
        try {
            Files.createDirectory(path);
            Files.createFile(path.resolve("latest"));
            Files.createFile(path.resolve("index"));
            return new HistoryManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static HistoryManager open(Path path) {
        if (!Files.isDirectory(path) ||
                !Files.exists(path.resolve("latest")) ||
                !Files.exists(path.resolve("index"))) {
            throw new InvalidStorePathException();
        }
        try {
            return new HistoryManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public void put(Hash hash, Set<Uid> uids) {
        append(hash, Operation.PUT, uids);
    }

    public void delete(Hash hash, Set<Uid> uids) {
        append(hash, Operation.DELETE, uids);
    }

    private void append(Hash hash, Operation operation, Set<Uid> uids) {
        TransactionContext txContext = currentTransactionContext();
        Event event = event()
                .withId(nextId.getAndIncrement())
                .withHash(hash)
                .withTimestamp(txContext.timestamp())
                .withOperation(operation)
                .withUids(uids)
                .build();

        byte[] bytes = bytes(event);
        if (txContext.fileLength(latest) + bytes.length > PAGE_SIZE) {
            Deque<Event> events = loadPage(latest);
            IndexEntry lastEntry = loadIndex().peekLast();
            String name = String.valueOf(lastEntry == null ? 1 : Integer.valueOf(lastEntry.name) + 1);

            txContext.move(latest, root.resolve(name));
            txContext.create(latest);
            try (Output output = txContext.openOutput(index)) {
                output.write(bytes(new IndexEntry(name,
                                                  events.getFirst().getId(),
                                                  events.getLast().getId())));
            }
        }
        try (Output output = txContext.openOutput(latest)) {
            output.write(bytes(event));
        }
    }

    public List<Event> history(boolean chronological) {
        LinkedList<Event> events = new LinkedList<>();
        Deque<Event> latestEvents = loadPage(latest);
        if (chronological) {
            addAll(events, latestEvents, chronological);
        }
        Deque<IndexEntry> entries = loadIndex();
        while (!entries.isEmpty()) {
            IndexEntry entry = chronological ? entries.removeLast() : entries.removeFirst();
            addAll(events, loadPage(root.resolve(entry.name)), chronological);
        }
        if (!chronological) {
            addAll(events, latestEvents, chronological);
        }
        return events;
    }

    private static void addAll(LinkedList<Event> events, Deque<Event> toAdd, boolean chronological) {
        for (Event event : toAdd) {
            if (chronological) {
                events.addLast(event);
            } else {
                events.addFirst(event);
            }
        }
    }

    private Deque<Event> loadPage(Path path) {
        Deque<Event> events = new LinkedList<>();
        try (StreamDecoder streamDecoder = new StreamDecoder(currentTransactionContext().openInput(path))) {
            while (streamDecoder.hasNext()) {
                Event event = readEvent(streamDecoder.next());
                events.add(event);
            }
        }
        return events;
    }

    private Deque<IndexEntry> loadIndex() {
        Deque<IndexEntry> entries = new LinkedList<>();
        try (StreamDecoder streamDecoder = new StreamDecoder(currentTransactionContext().openInput(index))) {
            while (streamDecoder.hasNext()) {
                entries.add(readEntry(streamDecoder.next()));
            }
        }
        return entries;
    }

    private static byte[] bytes(Event event) {
        List<byte[]> uids = new ArrayList<>();
        for (Uid uid : event.getUids()) {
            uids.add(uid.value());
        }
        return encoder()
                .put("id", event.getId())
                .put("hash", event.getHash().value())
                .put("timestamp", event.getTimestamp())
                .put("operation", event.getOperation().value())
                .put("uids", uids)
                .build();
    }

    private static Event readEvent(ObjectDecoder objectDecoder) {
        Set<Uid> uids = new HashSet<>();
        for (Object raw : objectDecoder.getList("uids")) {
            uids.add(new Uid((byte[]) raw));
        }
        return event()
                .withId(objectDecoder.getLong("id"))
                .withHash(new Hash(objectDecoder.getRaw("hash")))
                .withTimestamp(objectDecoder.getDate("timestamp"))
                .withOperation(Operation.of(objectDecoder.getByte("operation")))
                .withUids(uids)
                .build();
    }

    private static byte[] bytes(IndexEntry entry) {
        return encoder()
                .put("name", entry.name)
                .put("first", entry.first)
                .put("last", entry.last)
                .build();
    }

    private IndexEntry readEntry(ObjectDecoder objectDecoder) {
        String name = objectDecoder.getString("name");
        long first = objectDecoder.getLong("first");
        long last = objectDecoder.getLong("last");
        return new IndexEntry(name, first, last);
    }

    private class IndexEntry {

        public final String name;
        public final long first;
        public final long last;

        public IndexEntry(String name, long first, long last) {
            this.name = name;
            this.first = first;
            this.last = last;
        }
    }
}
