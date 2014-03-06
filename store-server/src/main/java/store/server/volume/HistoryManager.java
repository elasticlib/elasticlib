package store.server.volume;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import store.common.Event;
import store.common.Event.EventBuilder;
import store.common.Hash;
import store.common.Operation;
import store.common.io.ObjectDecoder;
import store.common.io.ObjectEncoder;
import store.common.value.Value;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;
import static store.server.transaction.TransactionManager.currentTransactionContext;

class HistoryManager {

    private static final int PAGE_SIZE = 8192;
    private final AtomicLong nextSeq;
    private final Path root;
    private final Path latest;
    private final Path index;

    private HistoryManager(Path root) throws IOException {
        this.root = root;
        latest = root.resolve("latest");
        index = root.resolve("index");
        nextSeq = new AtomicLong(initNextSeq());
    }

    private long initNextSeq() throws IOException {
        Deque<Event> latestEvents = loadPage(latest);
        if (!latestEvents.isEmpty()) {
            return latestEvents.getLast().getSeq() + 1;
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

    public void add(Hash hash, Operation operation, SortedSet<Hash> head) {
        TransactionContext txContext = currentTransactionContext();
        Event event = new EventBuilder()
                .withSeq(nextSeq.getAndIncrement())
                .withHash(hash)
                .withHead(head)
                .withTimestamp(new Date())
                .withOperation(operation)
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
                                                  events.getFirst().getSeq(),
                                                  events.getLast().getSeq())));
            }
        }
        try (Output output = txContext.openOutput(latest)) {
            output.write(bytes(event));
        }
    }

    public List<Event> history(boolean chronological, long first, int number) {
        Collector collector = new Collector(chronological, first, number);
        if (chronological) {
            return chronologicalHistory(collector);
        }
        return anteChronologicalHistory(collector);
    }

    private List<Event> chronologicalHistory(Collector collector) {
        Deque<Event> latestEvents = loadPage(latest);
        Deque<IndexEntry> entries = loadIndex();
        while (!entries.isEmpty() && !collector.isFull()) {
            IndexEntry entry = entries.removeFirst();
            if (entry.last > collector.cursor) {
                collector.addAll(loadPage(root.resolve(entry.name)));
            }
        }
        collector.addAll(latestEvents);
        return collector.getEvents();
    }

    private List<Event> anteChronologicalHistory(Collector collector) {
        Deque<Event> latestEvents = loadPage(latest);
        collector.addAll(latestEvents);
        if (collector.isFull()) {
            return collector.getEvents();
        }
        Deque<IndexEntry> entries = loadIndex();
        while (!entries.isEmpty() && !collector.isFull()) {
            IndexEntry entry = entries.removeLast();
            if (entry.first < collector.cursor) {
                collector.addAll(loadPage(root.resolve(entry.name)));
            }
        }
        return collector.getEvents();
    }

    private static class Collector {

        private final List<Event> events;
        private final boolean chronological;
        private long cursor;
        private int remainder;

        public Collector(boolean chronological, long first, int number) {
            events = new ArrayList<>();
            this.chronological = chronological;
            this.cursor = first;
            this.remainder = number;
        }

        public void addAll(Deque<Event> toAdd) {
            while (!toAdd.isEmpty() && !isFull()) {
                if (chronological) {
                    Event event = toAdd.removeFirst();
                    if (event.getSeq() > cursor) {
                        cursor = event.getSeq();
                        remainder--;
                        events.add(event);
                    }
                } else {
                    Event event = toAdd.removeLast();
                    if (event.getSeq() < cursor) {
                        cursor = event.getSeq();
                        remainder--;
                        events.add(event);
                    }
                }
            }
        }

        public long cursor() {
            return cursor;
        }

        public boolean isFull() {
            return remainder == 0;
        }

        public List<Event> getEvents() {
            return events;
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
        List<Value> head = new ArrayList<>();
        for (Hash rev : event.getHead()) {
            head.add(Value.of(rev.getBytes()));
        }
        return new ObjectEncoder()
                .put("seq", event.getSeq())
                .put("hash", event.getHash().getBytes())
                .put("head", head)
                .put("ts", event.getTimestamp())
                .put("code", event.getOperation().getCode())
                .build();
    }

    private static Event readEvent(ObjectDecoder objectDecoder) {
        SortedSet<Hash> head = new TreeSet<>();
        for (Value value : objectDecoder.getList("head")) {
            head.add(new Hash(value.asByteArray()));
        }
        return new EventBuilder()
                .withSeq(objectDecoder.getLong("seq"))
                .withHash(new Hash(objectDecoder.getByteArray("hash")))
                .withHead(head)
                .withTimestamp(objectDecoder.getDate("ts"))
                .withOperation(Operation.fromCode(objectDecoder.getByte("code")))
                .build();
    }

    private static byte[] bytes(IndexEntry entry) {
        return new ObjectEncoder()
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
