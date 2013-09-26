package store.server.info;

import com.google.common.base.Optional;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import static java.nio.file.Files.newInputStream;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static java.util.Collections.emptyList;
import java.util.LinkedList;
import java.util.List;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Hash;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.StoreRuntimeException;
import store.server.io.ObjectDecoder;
import static store.server.io.ObjectEncoder.encoder;
import store.server.io.StreamDecoder;

class Segment {

    private final Path path;

    public Segment(Path path) {
        this.path = path;
    }

    public synchronized void put(ContentInfo contentInfo) {
        if (contains(contentInfo.getHash())) {
            throw new ContentAlreadyStoredException();
        }
        try {
            Files.write(path, bytes(contentInfo), StandardOpenOption.APPEND, StandardOpenOption.CREATE);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public synchronized Optional<ContentInfo> get(Hash hash) {
        for (ContentInfo info : load()) {
            if (info.getHash().equals(hash)) {
                return Optional.of(info);
            }
        }
        return Optional.absent();
    }

    public synchronized boolean contains(Hash hash) {
        return get(hash).isPresent();
    }

    public synchronized void delete(Hash hash) {
        try {
            List<ContentInfo> list = load();
            if (list.size() == 1 && list.get(0).getHash().equals(hash)) {
                Files.delete(path);
            }
            try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(path))) {
                for (ContentInfo contentInfo : list) {
                    if (!contentInfo.getHash().equals(hash)) {
                        outputStream.write(bytes(contentInfo));
                    }
                }
            }
        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    private List<ContentInfo> load() {
        if (!Files.exists(path)) {
            return emptyList();
        }
        List<ContentInfo> list = new LinkedList<>();
        try (StreamDecoder streamDecoder = new StreamDecoder(new BufferedInputStream(newInputStream(path)))) {
            while (streamDecoder.hasNext()) {
                ObjectDecoder objectDecoder = streamDecoder.next();
                list.add(contentInfo()
                        .withHash(new Hash(objectDecoder.getRaw("hash")))
                        .withLength(objectDecoder.getLong("length"))
                        .withMetadata(objectDecoder.getMap("metadata"))
                        .build());
            }
            return list;

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
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
