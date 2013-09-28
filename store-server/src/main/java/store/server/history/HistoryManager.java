package store.server.history;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import store.common.Hash;
import store.server.Uid;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import static store.server.io.ObjectEncoder.encoder;
import store.server.transaction.Output;
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

    public void append(Hash hash, Operation operation, Set<Uid> uids) {
        Event event = new Event(hash, new Date().getTime(), operation, uids);
        byte[] bytes = encoder()
                .put("hash", event.getHash().value())
                .put("timestamp", event.getTimestamp())
                .put("operation", event.getOperation().value())
                .put("uids", bytes(uids))
                .build();

        try (Output output = currentTransactionContext().openAppendingOutput(path(hash))) {
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
}
