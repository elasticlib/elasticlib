package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.Digest;
import store.common.Digest.DigestBuilder;
import store.common.Hash;
import store.common.ContentInfo;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.WriteException;

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
            throw new StoreRuntimeException(e);
        }
    }

    public static ContentManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        return new ContentManager(path);
    }

    public void put(ContentInfo info, InputStream source) {
        Hash hash = info.getHash();
        Path path = createPath(hash);
        try {
            Digest digest = write(source, path);
            if (info.getLength() != digest.getLength() || !hash.equals(digest.getHash())) {
                throw new IntegrityCheckingFailedException();
            }
        } catch (StoreException e) {
            deleteFile(path);
            throw e;
        }
    }

    public InputStream get(Hash hash) {
        try {
            return Files.newInputStream(path(hash));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public void delete(Hash hash) {
        deleteFile(path(hash));
    }

    private static Digest write(InputStream source, Path path) throws WriteException {
        try (OutputStream target = Files.newOutputStream(path)) {
            DigestBuilder digestBuilder = new DigestBuilder();
            byte[] buffer = new byte[BUFFER_SIZE];
            int len = source.read(buffer);
            while (len != -1) {
                digestBuilder.add(buffer, len);
                target.write(buffer, 0, len);
                len = source.read(buffer);
            }
            return digestBuilder.build();

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private Path createPath(Hash hash) {
        try {
            Path dir = root.resolve(key(hash));
            if (!Files.exists(dir)) {
                Files.createDirectories(dir); // FIXME synchroniser ?
            }
            return dir.resolve(hash.encode());

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private Path path(Hash hash) {
        return root.resolve(key(hash)).resolve(hash.encode());
    }

    private void deleteFile(Path path) {
        try {
            Files.deleteIfExists(path);

        } catch (IOException e) {
            // TODO simplement logger l'erreur
        }
    }

    private static String key(Hash hash) {
        return hash.encode().substring(0, KEY_LENGTH);
    }
}
