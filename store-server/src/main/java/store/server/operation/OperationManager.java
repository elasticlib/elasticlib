package store.server.operation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import store.common.Hash;
import store.server.Uid;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.table.Table;

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

    public void beginPut(Uid uid, Hash hash) {
        add(uid, hash, OpCode.BEGIN_PUT);
    }

    public void endPut(Uid uid, Hash hash) {
        add(uid, hash, OpCode.END_PUT);
    }

    public void beginDelete(Uid uid, Hash hash) {
        add(uid, hash, OpCode.BEGIN_DELETE);
    }

    public void endDelete(Uid uid, Hash hash) {
        add(uid, hash, OpCode.END_DELETE);
    }

    private void add(Uid uid, Hash hash, OpCode opCode) {
        Segment segment = segments.get(hash);
        synchronized (segment) {
            segment.add(new Step(uid, hash, new Date().getTime(), opCode));
        }
    }
}
