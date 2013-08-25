package store.info;

import com.google.common.base.Optional;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReferenceArray;
import store.Index;
import store.exception.InvalidStorePathException;
import store.exception.StoreRuntimeException;
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

    private InfoManager(Path root) {
        this.root = root;

        cache = new AtomicReferenceArray<>(1 << (4 * KEY_LENGTH));
        for (int i = 0; i < cache.length(); i++) {
            cache.set(i, Page.unloaded());
        }
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
        int index = Index.of(key(contentInfo.getHash()));
        while (true) {
            Page page = cache.get(index);
            if (page.state() != LOCKED && cache.compareAndSet(index, page, Page.locked())) {
                try {
                    Files.write(root.resolve(key(contentInfo.getHash())),
                                bytes(contentInfo),
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    return;

                } catch (IOException e) {
                    throw new StoreRuntimeException(e);

                } finally {
                    cache.set(index, Page.unloaded());
                }
            }
        }
    }

    public Optional<ContentInfo> get(Hash hash) {
        return getPage(hash)
                .get(hash);
    }

    public boolean contains(Hash hash) {
        return getPage(hash)
                .contains(hash);
    }

    public void delete(Hash hash) {
        int index = Index.of(key(hash));
        while (true) {
            Page page = cache.get(index);
            Path bucket = root.resolve(key(hash));
            if (page.state() != LOCKED && cache.compareAndSet(index, page, Page.locked())) {
                try {
                    Page loaded = load(hash);
                    Collection<ContentInfo> contentInfos = loaded.getAll();
                    if (contentInfos.size() == 1 && contentInfos.iterator()
                            .next()
                            .getHash()
                            .equals(hash)) {

                        Files.delete(bucket);

                    } else {
                        try (OutputStream outputStream = Files.newOutputStream(bucket)) {
                            for (ContentInfo contentInfo : contentInfos) {
                                if (!hash.equals(contentInfo.getHash())) {
                                    outputStream.write(bytes(contentInfo));
                                }
                            }
                        }
                    }
                    return;

                } catch (IOException e) {
                    // FIXME Oh mon dieu, il fallait écrire dans un autre fichier et faire un move :'(
                    throw new StoreRuntimeException(e);

                } finally {
                    cache.set(index, Page.unloaded());
                }
            }
        }
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
                    throw new StoreRuntimeException(e);
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

    private static byte[] bytes(ContentInfo contentInfo) {
        Hash hash = contentInfo.getHash();
        return encoder()
                .put("hash", hash.value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();
    }
}
