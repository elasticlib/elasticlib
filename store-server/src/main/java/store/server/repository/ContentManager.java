package store.server.repository;

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
import store.common.exception.IOFailureException;
import store.common.exception.IntegrityCheckingFailedException;
import store.common.exception.InvalidRepositoryPathException;
import store.common.exception.UnknownContentException;
import store.common.hash.Digest;
import store.common.hash.Hash;
import static store.common.util.IoUtil.copyAndDigest;

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

    /**
     * Creates a new content manager.
     *
     * @param path content manager home.
     * @return Created content manager.
     */
    public static ContentManager create(Path path) {
        try {
            Files.createDirectory(path);
            for (String key : Hash.keySet(KEY_LENGTH)) {
                Files.createDirectory(path.resolve(key));
            }
            return new ContentManager(path);

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
    }

    /**
     * Opens an existing content manager.
     *
     * @param path content manager home.
     * @param Opened content manager.
     */
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

    /**
     * Closes this manager.
     */
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

    /**
     * Stores a new content.
     *
     * @param hash Content hash.
     * @param source Content bytes.
     */
    public void add(Hash hash, InputStream source) {
        lockManager.writeLock(hash);
        try (OutputStream target = newOutputStream(path(hash))) {
            Digest digest = copyAndDigest(source, target);
            if (!hash.equals(digest.getHash())) {
                throw new IntegrityCheckingFailedException();
            }
        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    /**
     * Deletes a currently stored content.
     *
     * @param hash Content hash.
     */
    public void delete(Hash hash) {
        lockManager.writeLock(hash);
        try {
            Files.delete(path(hash));

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    /**
     * Provides an input-stream on a currently stored content.
     *
     * @param hash Content hash.
     * @return An input-stream on this content.
     */
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
            throw new IOFailureException(e);
        }
    }

    private Path path(Hash hash) {
        return root
                .resolve(hash.key(KEY_LENGTH))
                .resolve(hash.asHexadecimalString());
    }

    /**
     * An input-stream wrapper that releases content read-lock when closed.
     */
    private class ContentInputStream extends InputStream {

        private final InputStream delegate;
        private final Hash hash;

        /**
         * Constructor.
         *
         * @param delegate Actual input-stream.
         * @param hash Content hash (for unlocking purpose).
         */
        public ContentInputStream(InputStream delegate, Hash hash) {
            this.delegate = delegate;
            this.hash = hash;
        }

        @Override
        public int read() {
            try {
                return delegate.read();

            } catch (IOException e) {
                throw new IOFailureException(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            try {
                return delegate.read(b, off, len);

            } catch (IOException e) {
                throw new IOFailureException(e);
            }
        }

        @Override
        public int available() {
            try {
                return delegate.available();

            } catch (IOException e) {
                throw new IOFailureException(e);
            }
        }

        @Override
        public void close() {
            try {
                delegate.close();

            } catch (IOException e) {
                throw new IOFailureException(e);

            } finally {
                inputStreams.remove(this);
                lockManager.readUnlock(hash);
            }
        }
    }
}
