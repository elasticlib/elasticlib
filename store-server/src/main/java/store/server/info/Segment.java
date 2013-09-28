package store.server.info;

import com.google.common.base.Optional;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.util.Collections.emptyList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Hash;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.UnknownHashException;
import store.server.io.ObjectDecoder;
import static store.server.io.ObjectEncoder.encoder;
import store.server.io.StreamDecoder;
import store.server.transaction.Output;
import static store.server.transaction.TransactionManager.currentTransactionContext;

final class Segment {

    private final Path path;

    public Segment(Path path) {
        this.path = path;
    }

    public void put(ContentInfo contentInfo) {
        if (contains(contentInfo.getHash())) {
            throw new ContentAlreadyStoredException();
        }
        try (Output output = currentTransactionContext().openAppendingOutput(path)) {
            output.write(bytes(contentInfo));
        }
    }

    public Optional<ContentInfo> get(Hash hash) {
        for (ContentInfo info : load()) {
            if (info.getHash().equals(hash)) {
                return Optional.of(info);
            }
        }
        return Optional.absent();
    }

    public boolean contains(Hash hash) {
        return get(hash).isPresent();
    }

    public void delete(Hash hash) {
        List<ContentInfo> list = load();
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

    private List<ContentInfo> load() {
        if (!Files.exists(path)) {
            return emptyList();
        }
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
