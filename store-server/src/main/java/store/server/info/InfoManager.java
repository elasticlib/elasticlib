package store.server.info;

import com.google.common.base.Optional;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.LinkedList;
import store.common.Hash;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import static store.server.info.PageState.*;
import store.server.io.ObjectDecoder;
import static store.server.io.ObjectEncoder.encoder;
import store.server.io.StreamDecoder;
import store.server.table.AtomicTable;

public class InfoManager {

    private static final int KEY_LENGTH = 2;
    private final Path root;
    private final AtomicTable<Page> cache;

    private InfoManager(Path root) {
        this.root = root;
        cache = new AtomicTable<Page>(KEY_LENGTH) {
            @Override
            protected Page initialValue() {
                return Page.unloaded();
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
        Hash hash = contentInfo.getHash();
        while (true) {
            Page page = cache.get(hash);
            if (page.state() != LOCKED && cache.compareAndSet(hash, page, Page.locked())) {
                try {
                    if (page.state() == UNLOADED) {
                        page = load(hash);
                    }
                    if (page.contains(hash)) {
                        throw new ContentAlreadyStoredException();
                    }
                    Files.write(root.resolve(key(contentInfo.getHash())),
                                bytes(contentInfo),
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    return;

                } catch (IOException e) {
                    throw new StoreRuntimeException(e);

                } finally {
                    cache.set(hash, Page.unloaded());
                }
            }
        }
    }

    public Optional<ContentInfo> get(Hash hash) {
        return getPage(hash).get(hash);
    }

    public boolean contains(Hash hash) {
        return getPage(hash).contains(hash);
    }

    public void delete(Hash hash) {
        while (true) {
            Page page = cache.get(hash);
            Path bucket = root.resolve(key(hash));
            if (page.state() != LOCKED && cache.compareAndSet(hash, page, Page.locked())) {
                try {
                    if (page.state() == UNLOADED) {
                        page = load(hash);
                    }
                    Collection<ContentInfo> contentInfos = page.getAll();
                    if (contentInfos.size() == 1 && contentInfos.iterator().next().getHash().equals(hash)) {
                        Files.delete(bucket);

                    } else {
                        try (OutputStream outputStream = Files.newOutputStream(bucket)) {
                            for (ContentInfo contentInfo : contentInfos) {
                                if (!contentInfo.getHash().equals(hash)) {
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
                    cache.set(hash, Page.unloaded());
                }
            }
        }
    }

    private Page getPage(Hash hash) {
        while (true) {
            Page page = cache.get(hash);
            if (page.state() == LOADED) {
                return page;
            }
            if (page.state() != LOCKED && cache.compareAndSet(hash, page, Page.locked())) {
                try {
                    Page loaded = load(hash);
                    // FIXME si il y a trop de pages chargées, il faut en décharger une !
                    cache.set(hash, loaded);
                    return loaded;

                } catch (IOException e) {
                    cache.set(hash, Page.unloaded());
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
        return hash.encode().substring(0, KEY_LENGTH);
    }

    private static byte[] bytes(ContentInfo contentInfo) {
        return encoder()
                .put("hash", contentInfo.getHash().value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();
    }
}
