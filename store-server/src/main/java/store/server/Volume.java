package store.server;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.Hash;
import store.common.ContentInfo;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.info.InfoManager;

public class Volume {

    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Volume(InfoManager infoManager, ContentManager contentManager) {
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    public static Volume create(Path path) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidStorePathException();
            }
            return new Volume(InfoManager.create(path.resolve("info")),
                              ContentManager.create(path.resolve("content")));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    public static Volume open(Path path) {
        return new Volume(InfoManager.open(path.resolve("info")),
                          ContentManager.open(path.resolve("content")));
    }

    public boolean contains(Hash hash) {
        return infoManager.contains(hash);
    }

    public void put(ContentInfo contentInfo, InputStream source) {
        infoManager.put(contentInfo);
        contentManager.put(contentInfo, source);
    }

    public Optional<ContentInfo> info(Hash hash) {
        return infoManager.get(hash);
    }

    public Optional<Content> get(Hash hash) {
        Optional<ContentInfo> info = infoManager.get(hash);
        if (info.isPresent()) {
            return Optional.of(new Content(info.get(), contentManager.get(hash)));
        }
        return Optional.absent();
    }

    public void erase(Hash hash) {
        infoManager.delete(hash);
        contentManager.delete(hash);
    }
}
