package store.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import store.common.Event;
import static store.common.Event.event;
import store.common.Hash;
import store.common.Operation;
import store.common.Uid;
import static store.server.Table.keySet;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.io.ObjectDecoder;
import static store.server.io.ObjectEncoder.encoder;
import store.server.io.StreamDecoder;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;
import static store.server.transaction.TransactionManager.currentTransactionContext;

public class HistoryManager {

    private static final int KEY_LENGTH = 1;
    private final Path root;

    private HistoryManager(final Path root) {
        this.root = root;
    }

    public static HistoryManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new HistoryManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static HistoryManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        return new HistoryManager(path);
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
                .withHash(hash)
                .withTimestamp(txContext.timestamp())
                .withOperation(operation)
                .withUids(uids)
                .build();

        byte[] bytes = encoder()
                .put("hash", event.getHash().value())
                .put("timestamp", event.getTimestamp())
                .put("operation", event.getOperation().value())
                .put("uids", bytes(uids))
                .build();

        Path path = path(hash);
        if (!txContext.exists(path)) {
            txContext.create(path);
        }
        try (Output output = txContext.openOutput(path(hash))) {
            output.write(bytes);
        }
    }

    private static List<byte[]> bytes(Set<Uid> uids) {
        List<byte[]> list = new ArrayList<>();
        for (Uid uid : uids) {
            list.add(uid.value());
        }
        return list;
    }

    private Path path(Hash hash) {
        String key = hash.encode().substring(0, KEY_LENGTH);
        return root.resolve(key);
    }

    public List<Event> history(boolean chronological) {
        List<Event> events = new LinkedList<>();
        Sorter sorter = new Sorter(chronological);
        for (String key : keySet(KEY_LENGTH)) {
            Path path = root.resolve(key);
            if (currentTransactionContext().exists(path)) {
                load(events, path, sorter);
            }
        }
        return events;
    }

    private static List<Event> load(List<Event> events, Path path, Sorter sorter) {
        try (StreamDecoder streamDecoder = new StreamDecoder(currentTransactionContext().openInput(path))) {
            while (streamDecoder.hasNext()) {
                ObjectDecoder objectDecoder = streamDecoder.next();
                Set<Uid> uids = new HashSet<>();
                for (Object raw : objectDecoder.getList("uids")) {
                    uids.add(new Uid((byte[]) raw));
                }
                Event event = event()
                        .withHash(new Hash(objectDecoder.getRaw("hash")))
                        .withTimestamp(objectDecoder.getDate("timestamp"))
                        .withOperation(Operation.of(objectDecoder.getByte("operation")))
                        .withUids(uids)
                        .build();
                insert(events, event, sorter);
            }
        }
        return events;
    }

    private static void insert(List<Event> list, Event item, Sorter sorter) {
        ListIterator<Event> it = list.listIterator();
        while (it.hasNext()) {
            if (!sorter.isOrdered(it.next(), item)) {
                list.add(it.previousIndex(), item);
                return;
            }
        }
        list.add(item);
    }

    private static class Sorter {

        private final boolean chronological;

        public Sorter(boolean chronological) {
            this.chronological = chronological;
        }

        public boolean isOrdered(Event e1, Event e2) {
            Date t1 = e1.getTimestamp();
            Date t2 = e2.getTimestamp();
            return chronological ? t1.before(t2) : t1.after(t2);
        }
    }
}
