package store;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.hash.Hash;

public class Store {

    private final InfoManager infoManager;

    private Store(Path root) {
        infoManager = new InfoManager(root.resolve("info"));
    }

    public static Store create(Path path) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
            if (!isEmptyDir(path)) {
                throw new IllegalArgumentException(path.toString());
            }
            Files.createDirectory(path.resolve("content"));
            Files.createDirectory(path.resolve("info"));
            return new Store(path);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator()
                    .hasNext();
        }
    }

    public static Store open(Path path) {
        if (!Files.isDirectory(path.resolve("content")) || !Files.isDirectory(path.resolve("info"))) {
            throw new IllegalArgumentException(path.toString());
        }
        return new Store(path);
    }

    public ContentWriter put(ContentInfo contentInfo) {
        infoManager.add(contentInfo);
        return new ContentWriter();
    }

    public ContentReader get(Hash hash) {
        return new ContentReader(infoManager.get(hash));
    }

    public void delete(Hash hash) {
        // TODO
    }
}
