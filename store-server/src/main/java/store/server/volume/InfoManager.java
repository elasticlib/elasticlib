package store.server.volume;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.Hash;
import store.common.io.ObjectDecoder;
import store.common.io.ObjectEncoder;
import store.common.value.Value;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;
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
        Path path = path(hash);
        TransactionContext txContext = currentTransactionContext();
        if (!txContext.exists(path)) {
            txContext.create(path);
        }
        try (Output output = txContext.openOutput(path)) {
            output.write(bytes(contentInfo));
        }
    }

    public void delete(Hash hash) {
        TransactionContext txContext = currentTransactionContext();
        Path path = path(hash);
        if (!txContext.exists(path)) {
            throw new UnknownHashException();
        }
        List<Entry> entries = load(path);
        if (entries.size() == 1 && entries.get(0).getPosition() == 0) {
            txContext.delete(path);
            return;
        }
        Iterator<Entry> it = entries.iterator();
        while (it.hasNext()) {
            Entry entry = it.next();
            if (entry.matches(hash)) {
                txContext.truncate(path, entry.getPosition());
                if (it.hasNext()) {
                    try (Output output = txContext.openOutput(path)) {
                        while (it.hasNext()) {
                            output.write(bytes(it.next().getInfo()));
                        }
                    }
                }
                return;
            }
        }
        throw new UnknownHashException();
    }

    public boolean contains(Hash hash) {
        return get(hash).isPresent();
    }

    public Optional<ContentInfo> get(Hash hash) {
        Path path = path(hash);
        if (!currentTransactionContext().exists(path)) {
            return Optional.absent();
        }
        for (Entry entry : load(path)) {
            if (entry.matches(hash)) {
                return Optional.of(entry.getInfo());
            }
        }
        return Optional.absent();
    }

    private Path path(Hash hash) {
        String key = hash.encode().substring(0, KEY_LENGTH);
        return root.resolve(key);
    }

    private static byte[] bytes(ContentInfo contentInfo) {
        ObjectEncoder encoder = new ObjectEncoder()
                .put("hash", contentInfo.getHash().value())
                .put("rev", contentInfo.getRev().value());

        List<Value> list = new ArrayList<>();
        for (Hash parent : contentInfo.getParents()) {
            list.add(Value.of(parent.value()));
        }
        encoder.put("parents", list);
        if (contentInfo.isDeleted()) {
            encoder.put("deleted", true);
        }
        return encoder
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();
    }

    private static List<Entry> load(Path path) {
        List<Entry> entries = new LinkedList<>();
        try (StreamDecoder streamDecoder = new StreamDecoder(currentTransactionContext().openInput(path))) {
            while (streamDecoder.hasNext()) {
                ObjectDecoder objectDecoder = streamDecoder.next();
                ContentInfoBuilder builder = new ContentInfoBuilder()
                        .withHash(new Hash(objectDecoder.getRaw("hash")))
                        .withRev(new Hash(objectDecoder.getRaw("rev")));
                for (Object obj : objectDecoder.getList("parents")) {
                    builder.withParent(new Hash((byte[]) obj));
                }
                if (objectDecoder.containsKey("deleted")) {
                    builder.withDeleted(objectDecoder.getBoolean("deleted"));
                }
                ContentInfo info = builder
                        .withLength(objectDecoder.getLong("length"))
                        .withMetadata(objectDecoder.getMap("metadata"))
                        .build();
                entries.add(new Entry(streamDecoder.position(), info));
            }
        }
        return entries;
    }

    private static final class Entry {

        private final long position;
        private final ContentInfo info;

        public Entry(long position, ContentInfo info) {
            this.position = position;
            this.info = info;
        }

        public long getPosition() {
            return position;
        }

        public ContentInfo getInfo() {
            return info;
        }

        public boolean matches(Hash hash) {
            return info.getHash().equals(hash);
        }
    }
}
