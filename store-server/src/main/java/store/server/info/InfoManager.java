package store.server.info;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.ContentInfo;
import store.common.Hash;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.Table;

public class InfoManager {

    private static final int KEY_LENGTH = 1;
    private final Table<Segment> segments;

    private InfoManager(final Path root) {
        segments = new Table<Segment>(KEY_LENGTH) {
            @Override
            protected Segment initialValue(String key) {
                return new Segment(root.resolve(key));
            }
        };
    }

    public static InfoManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new InfoManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static InfoManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        return new InfoManager(path);
    }

    public void put(ContentInfo contentInfo) {
        segments.get(contentInfo.getHash()).put(contentInfo);
    }

    public Optional<ContentInfo> get(Hash hash) {
        return segments.get(hash).get(hash);
    }

    public boolean contains(Hash hash) {
        return segments.get(hash).contains(hash);
    }

    public void delete(Hash hash) {
        segments.get(hash).delete(hash);
    }
}
