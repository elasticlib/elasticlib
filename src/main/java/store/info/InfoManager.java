package store.info;

import store.Index;
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

    private static final int KEY_LENGTH = 2;
    private final Path root;
    private final AtomicReferenceArray<Page> cache;

    public InfoManager(Path root) {
        this.root = root;

        cache = new AtomicReferenceArray<>(1 << (4 * KEY_LENGTH));
        for (int i = 0; i < cache.length(); i++) {
            cache.set(i, Page.unloaded());
        }
    }

    public void put(ContentInfo contentInfo) {
        byte[] bytes = encoder()
                .put("hash", contentInfo.getHash()
                .value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();

        int index = Index.of(key(contentInfo.getHash()));
        while (true) {
            Page page = cache.get(index);
            if (page.state() != LOCKED && cache.compareAndSet(index, page, Page.locked())) {
                try {
                    Files.write(root.resolve(key(contentInfo.getHash())),
                                bytes,
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    return;

                } catch (IOException e) {
                    throw new RuntimeException(e);

                } finally {
                    cache.set(index, Page.unloaded());
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
        int index = Index.of(key(hash));
        while (true) {
            Page page = cache.get(index);
            if (page.state() == LOADED) {
                return page;
            }
            if (page.state() != LOCKED && cache.compareAndSet(index, page, Page.locked())) {
                try {
                    Page loaded = load(hash);
                    // FIXME si il y a trop de pages chargées, il faut en décharger une !
                    cache.set(index, loaded);
                    return loaded;

                } catch (IOException e) {
                    cache.set(index, Page.unloaded());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private Page load(Hash hash) throws IOException {
        Path bucket = root.resolve(key(hash));
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

    private static String key(Hash hash) {
        return hash.encode()
                .substring(0, KEY_LENGTH);
    }
}
