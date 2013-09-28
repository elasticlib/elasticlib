package store.server.volume;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Hash;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import store.server.io.ObjectDecoder;
import static store.server.io.ObjectEncoder.encoder;
import store.server.io.StreamDecoder;
import store.server.transaction.Output;
import static store.server.transaction.TransactionManager.currentTransactionContext;

class InfoManager {

    private static final int KEY_LENGTH = 2;
    private final Path root;

    private InfoManager(final Path root) {
        this.root = root;
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
        Hash hash = contentInfo.getHash();
        if (contains(hash)) {
            throw new ContentAlreadyStoredException();
        }
        try (Output output = currentTransactionContext().openAppendingOutput(path(hash))) {
            output.write(bytes(contentInfo));
        }
    }

    public void delete(Hash hash) {
        Path path = path(hash);
        List<ContentInfo> list = load(path);
        remove(list, hash);
        if (list.isEmpty()) {
            currentTransactionContext().delete(path);

        } else {
            try (Output output = currentTransactionContext().openTruncatingOutput(path)) {
                for (ContentInfo contentInfo : list) {
                    output.write(bytes(contentInfo));
                }
            }
        }
    }

    private static void remove(List<ContentInfo> list, Hash hash) {
        Iterator<ContentInfo> it = list.listIterator();
        while (it.hasNext()) {
            if (it.next().getHash().equals(hash)) {
                it.remove();
                return;
            }
        }
        throw new UnknownHashException();
    }

    public boolean contains(Hash hash) {
        return get(hash).isPresent();
    }

    public Optional<ContentInfo> get(Hash hash) {
        for (ContentInfo info : load(path(hash))) {
            if (info.getHash().equals(hash)) {
                return Optional.of(info);
            }
        }
        return Optional.absent();
    }

    private Path path(Hash hash) {
        String key = hash.encode().substring(0, KEY_LENGTH);
        return root.resolve(key);
    }

    private static List<ContentInfo> load(Path path) {
        List<ContentInfo> list = new LinkedList<>();
        try (StreamDecoder streamDecoder = new StreamDecoder(currentTransactionContext().openInput(path))) {
            while (streamDecoder.hasNext()) {
                ObjectDecoder objectDecoder = streamDecoder.next();
                list.add(contentInfo()
                        .withHash(new Hash(objectDecoder.getRaw("hash")))
                        .withLength(objectDecoder.getLong("length"))
                        .withMetadata(objectDecoder.getMap("metadata"))
                        .build());
            }
            return list;
        }
    }

    private static byte[] bytes(ContentInfo contentInfo) {
        return encoder()
                .put("hash", contentInfo.getHash().value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();
    }
}
