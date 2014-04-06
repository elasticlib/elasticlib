package store.server.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import org.joda.time.Instant;
import store.common.Event;
import store.common.Event.EventBuilder;
import store.common.Hash;
import store.common.Operation;
import store.common.bson.BsonReader;
import store.common.bson.BsonWriter;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.WriteException;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;

class HistoryManager {

    private static final int PAGE_SIZE = 8192;
    private final AtomicLong nextSeq;
    private final Path root;
    private final Path latest;
    private final Path index;

    private HistoryManager(Path root) {
        this.root = root;
        latest = root.resolve("latest");
        index = root.resolve("index");
        nextSeq = new AtomicLong(initNextSeq());
    }

    private long initNextSeq() {
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
            throw new WriteException(e);
        }
    }

    public static HistoryManager open(Path path) {
        if (!Files.isDirectory(path) ||
                !Files.exists(path.resolve("latest")) ||
                !Files.exists(path.resolve("index"))) {
            throw new InvalidRepositoryPathException();
        }
        return new HistoryManager(path);
    }

    public void add(Hash content, Operation operation, SortedSet<Hash> head) {
        TransactionContext txContext = TransactionContext.current();
        Event event = new EventBuilder()
                .withSeq(nextSeq.getAndIncrement())
                .withContent(content)
                .withHead(head)
                .withTimestamp(new Instant())
                .withOperation(operation)
                .build();

        byte[] bytes = new BsonWriter()
                .put(event.toMap())
                .build();

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
            output.write(bytes);
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
        try (BsonStreamReader streamDecoder = new BsonStreamReader(TransactionContext.current().openInput(path))) {
            while (streamDecoder.hasNext()) {
                Event event = Event.fromMap(streamDecoder.next().asMap());
                events.add(event);
            }
        }
        return events;
    }

    private Deque<IndexEntry> loadIndex() {
        Deque<IndexEntry> entries = new LinkedList<>();
        try (BsonStreamReader streamReader = new BsonStreamReader(TransactionContext.current().openInput(index))) {
            while (streamReader.hasNext()) {
                entries.add(readEntry(streamReader.next()));
            }
        }
        return entries;
    }

    private static byte[] bytes(IndexEntry entry) {
        return new BsonWriter()
                .put("name", entry.name)
                .put("first", entry.first)
                .put("last", entry.last)
                .build();
    }

    private IndexEntry readEntry(BsonReader reader) {
        String name = reader.getString("name");
        long first = reader.getLong("first");
        long last = reader.getLong("last");
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
