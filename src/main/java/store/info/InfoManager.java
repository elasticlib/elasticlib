package store.info;

import com.google.common.base.Optional;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReferenceArray;
import store.hash.Hash;
import static store.info.ContentInfo.contentInfo;
import static store.info.PageState.*;
import store.io.ObjectDecoder;
import static store.io.ObjectEncoder.encoder;
import store.io.StreamDecoder;

public class InfoManager {

    private final Path root;
    private final AtomicReferenceArray<Page> buffer;

    public InfoManager(Path root) {
        this.root = root;

        buffer = new AtomicReferenceArray<>(256);
        for (int i = 0; i < buffer.length(); i++) {
            buffer.set(i, Page.unloaded());
        }
    }

    public void put(ContentInfo contentInfo) {
        byte[] bytes = encoder()
                .put("hash", contentInfo.getHash()
                .value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();

        int index = IndexResolver.index(contentInfo.getHash());
        while (true) {
            Page page = buffer.get(index);
            if (page.state() != LOCKED && buffer.compareAndSet(index, page, Page.locked())) {
                try {
                    Files.write(bucket(contentInfo.getHash()),
                                bytes,
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    return;

                } catch (IOException e) {
                    throw new RuntimeException(e);

                } finally {
                    buffer.set(index, Page.unloaded());
                }
            }
        }
    }

    public ContentInfo get(Hash hash) {
        Page page = getPage(hash);
        Optional<ContentInfo> contentInfo = page.get(hash);
        if (!contentInfo.isPresent()) {
            throw new IllegalArgumentException("Unknown hash");
        }
        return contentInfo.get();
    }

    private Page getPage(Hash hash) {
        int index = IndexResolver.index(hash);
        while (true) {
            Page page = buffer.get(index);
            if (page.state() == LOADED) {
                return page;
            }
            if (page.state() != LOCKED && buffer.compareAndSet(index, page, Page.locked())) {
                try {
                    Page loaded = load(hash);
                    // FIXME si il y a trop de pages chargées, il faut en décharger une !
                    buffer.set(index, loaded);
                    return loaded;

                } catch (IOException e) {
                    buffer.set(index, Page.unloaded());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private Page load(Hash hash) throws IOException {
        Path bucket = bucket(hash);
        if (!Files.exists(bucket)) {
            return Page.empty();
        }
        try (StreamDecoder streamDecoder = streamDecoder(bucket)) {
            Collection<ContentInfo> contentInfos = new LinkedList<>();
            while (streamDecoder.hasNext()) {
                ObjectDecoder objectDecoder = streamDecoder.next();
                contentInfos.add(contentInfo()
                        .withHash(new Hash(objectDecoder.getRaw("hash")))
                        .withLength(objectDecoder.getLong("length"))
                        .withMetadata(objectDecoder.getMap("metadata"))
                        .build());
            }
            return Page.of(contentInfos);
        }
    }

    private static StreamDecoder streamDecoder(Path path) throws IOException {
        return new StreamDecoder(new BufferedInputStream(Files.newInputStream(path)));
    }

    private Path bucket(Hash hash) {
        return root.resolve(hash.encode()
                .substring(0, 2));
    }
}
