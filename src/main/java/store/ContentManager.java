package store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.hash.Digest;
import store.hash.Digest.DigestBuilder;
import store.hash.Hash;
import store.info.ContentInfo;

public class ContentManager {

    private static final int BUFFER_SIZE = 65536;
    private static final int KEY_LENGTH = 2;
    private final Path root;

    private ContentManager(Path root) {
        this.root = root;
    }

    public static ContentManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new ContentManager(path);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ContentManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException(path.toString());
        }
        return new ContentManager(path);
    }

    public void put(ContentInfo info, InputStream source) {
        Hash hash = info.getHash();
        Path file = file(hash);
        try (OutputStream target = Files.newOutputStream(file)) {
            Digest digest = copy(source, target);
            if (info.getLength() != digest.getLength() || !hash.equals(digest.getHash())) {
                throw new IOException("Echec de validation de l'empreinte du contenu");
            }
        } catch (IOException e) {
            delete(file);
            throw new RuntimeException(e);
        }
    }

    private static Digest copy(InputStream source, OutputStream target) throws IOException {
        DigestBuilder digestBuilder = new DigestBuilder();
        byte[] buffer = new byte[BUFFER_SIZE];
        int len = source.read(buffer);
        while (len != -1) {
            digestBuilder.add(buffer, len);
            target.write(buffer, 0, len);
            len = source.read(buffer);
        }
        return digestBuilder.build();
    }

    private Path file(Hash hash) {
        try {
            Path dir = root.resolve(key(hash));
            if (!Files.exists(dir)) {
                Files.createDirectories(dir); // FIXME synchroniser ?
            }
            return dir.resolve(hash.encode());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void delete(Path path) {
        try {
            Files.deleteIfExists(path);

        } catch (IOException e) {
            // TODO simplement logger l'erreur
        }
    }

    private static String key(Hash hash) {
        return hash.encode()
                .substring(0, KEY_LENGTH);
    }
}
