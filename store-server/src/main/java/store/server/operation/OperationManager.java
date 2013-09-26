package store.server.operation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.Hash;
import store.server.Uid;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.Table;

public class OperationManager {

    private static final int KEY_LENGTH = 1;
    private final Table<Segment> segments;

    private OperationManager(final Path root) {
        segments = new Table<Segment>(KEY_LENGTH) {
            @Override
            protected Segment initialValue(String key) {
                return new Segment(root.resolve(key));
            }
        };
    }

    public static OperationManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new OperationManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static OperationManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        return new OperationManager(path);
    }

    public void put(Uid uid, Hash hash) {
        segments.get(hash).add(uid, hash, OpCode.PUT);
    }

    public void delete(Uid uid, Hash hash) {
        segments.get(hash).add(uid, hash, OpCode.DELETE);
    }
}
