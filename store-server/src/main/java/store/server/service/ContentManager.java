package store.server.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static store.common.IoUtil.copyAndDigest;
import store.common.hash.Digest;
import store.common.hash.Hash;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;

/**
 * Stores and retrieves contents inside a repository.
 */
class ContentManager {

    private static final int KEY_LENGTH = 2;
    private static final Logger LOG = LoggerFactory.getLogger(ContentManager.class);
    private final LockManager lockManager = new LockManager();
    private final Deque<InputStream> inputStreams = new ConcurrentLinkedDeque<>();
    private final Path root;

    private ContentManager(Path root) {
        this.root = root;
    }

    public static ContentManager create(Path path) {
        try {
            Files.createDirectory(path);
            for (String key : Hash.keySet(KEY_LENGTH)) {
                Files.createDirectory(path.resolve(key));
            }
            return new ContentManager(path);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    public static ContentManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        for (String key : Hash.keySet(KEY_LENGTH)) {
            if (!Files.isDirectory(path.resolve(key))) {
                throw new InvalidRepositoryPathException();
            }
        }
        return new ContentManager(path);
    }

    public void close() {
        lockManager.close();
        while (!inputStreams.isEmpty()) {
            try {
                inputStreams.remove().close();

            } catch (Exception e) {
                LOG.error("Failed to close input stream", e);
            }
        }
    }

    public void add(Hash hash, long length, InputStream source) {
        lockManager.writeLock(hash);
        try (OutputStream target = newOutputStream(path(hash))) {
            Digest digest = copyAndDigest(source, target);
            if (length != digest.getLength() || !hash.equals(digest.getHash())) {
                throw new IntegrityCheckingFailedException();
            }
        } catch (IOException e) {
            throw new WriteException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    public void delete(Hash hash) {
        lockManager.writeLock(hash);
        try {
            Files.delete(path(hash));

        } catch (IOException e) {
            throw new WriteException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    public InputStream get(Hash hash) {
        lockManager.readLock(hash);
        try {
            InputStream inputStream = new ContentInputStream(newInputStream(path(hash)), hash);
            inputStreams.add(inputStream);
            return inputStream;

        } catch (NoSuchFileException e) {
            lockManager.readUnlock(hash);
            throw new UnknownContentException(e);

        } catch (IOException e) {
            lockManager.readUnlock(hash);
            throw new WriteException(e);
        }
    }

    private Path path(Hash hash) {
        return root
                .resolve(hash.key(KEY_LENGTH))
                .resolve(hash.asHexadecimalString());
    }

    private class ContentInputStream extends InputStream {

        private final InputStream delegate;
        private final Hash hash;

        public ContentInputStream(InputStream delegate, Hash hash) {
            this.delegate = delegate;
            this.hash = hash;
        }

        @Override
        public int read() {
            try {
                return delegate.read();

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            try {
                return delegate.read(b, off, len);

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        @Override
        public int available() {
            try {
                return delegate.available();

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        @Override
        public void close() {
            try {
                delegate.close();

            } catch (IOException e) {
                throw new WriteException(e);

            } finally {
                inputStreams.remove(this);
                lockManager.readUnlock(hash);
            }
        }
    }
}
