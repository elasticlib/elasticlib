package org.elasticlib.node.repository;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import static java.nio.file.Files.newOutputStream;
import java.nio.file.Path;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.BadRequestException;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.IntegrityCheckingFailedException;
import org.elasticlib.common.exception.InvalidRepositoryPathException;
import org.elasticlib.common.exception.PendingStagingSessionException;
import org.elasticlib.common.exception.StagingCompletedException;
import org.elasticlib.common.exception.UnknownContentException;
import org.elasticlib.common.hash.Digest;
import org.elasticlib.common.hash.Digest.DigestBuilder;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.util.BoundedInputStream;
import static org.elasticlib.common.util.IoUtil.copyAndDigest;
import org.elasticlib.common.util.RandomAccessFileOutputStream;
import static org.elasticlib.common.util.SinkOutputStream.sink;
import org.elasticlib.node.manager.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores and retrieves contents inside a repository.
 */
class ContentManager {

    private static final int KEY_LENGTH = 2;
    private static final String STAGE = "stage";
    private static final String CONTENT = "content";
    private static final Logger LOG = LoggerFactory.getLogger(ContentManager.class);

    private final Path root;
    private final LockManager lockManager;
    private final Deque<InputStream> inputStreams;
    private final StagingSessionsCache sessions;

    private ContentManager(String name, Path root, Config config, TaskManager taskManager) {
        this.root = root;
        lockManager = new LockManager();
        inputStreams = new ConcurrentLinkedDeque<>();
        sessions = new StagingSessionsCache(name, config, taskManager);
    }

    /**
     * Creates a new content manager.
     *
     *
     * @param name repository name.
     * @param path repository path.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @return Created content manager.
     */
    public static ContentManager create(String name, Path path, Config config, TaskManager taskManager) {
        try {
            Files.createDirectory(path.resolve(STAGE));
            Files.createDirectory(path.resolve(CONTENT));
            for (String key : Hash.keySet(KEY_LENGTH)) {
                Files.createDirectory(path.resolve(CONTENT).resolve(key));
            }
            return new ContentManager(name, path, config, taskManager);

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
    }

    /**
     * Opens an existing content manager.
     *
     * @param name repository name.
     * @param path repository path.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param Opened content manager.
     */
    public static ContentManager open(String name, Path path, Config config, TaskManager taskManager) {
        if (!Files.isDirectory(path.resolve(STAGE)) || !Files.isDirectory(path.resolve(CONTENT))) {
            throw new InvalidRepositoryPathException();
        }
        for (String key : Hash.keySet(KEY_LENGTH)) {
            if (!Files.isDirectory(path.resolve(CONTENT).resolve(key))) {
                throw new InvalidRepositoryPathException();
            }
        }
        return new ContentManager(name, path, config, taskManager);
    }

    /**
     * Closes this manager.
     */
    public void close() {
        lockManager.close();
        sessions.close();
        while (!inputStreams.isEmpty()) {
            try {
                inputStreams.remove().close();

            } catch (Exception e) {
                LOG.error("Failed to close input stream", e);
            }
        }
    }

    /**
     * Creates a new staging session for the content which hash is supplied.
     *
     * @param hash Hash of the content to be added latter.
     * @return Info about the session process created.
     */
    public StagingInfo stageContent(Hash hash) {
        lockManager.writeLock(hash);
        try {
            DigestBuilder digest = loadDigest(hash);
            Guid sessionId = Guid.random();
            sessions.save(hash, new StagingSession(sessionId, digest));
            return new StagingInfo(sessionId, digest.getHash(), digest.getLength());

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    private DigestBuilder loadDigest(Hash hash) throws IOException {
        Optional<StagingSession> session = sessions.get(hash);
        if (session.isPresent()) {
            return reuseDigest(session.get(), hash);
        }
        DigestBuilder digest = computeDigest(hash, Long.MAX_VALUE);
        if (digest.getHash().equals(hash)) {
            throw new StagingCompletedException();
        }
        return digest;
    }

    private static DigestBuilder reuseDigest(StagingSession session, Hash hash) {
        if (session.getDigest().getHash().equals(hash)) {
            throw new StagingCompletedException();
        }
        if (session.getSessionId() != null) {
            throw new PendingStagingSessionException();
        }
        return session.getDigest();
    }

    /**
     * Writes bytes to a staged content.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     * @param source Bytes to write.
     * @param position Position in staged content at which write should begin.
     * @return Updated info of the staging session.
     */
    public StagingInfo writeContent(Hash hash, Guid sessionId, InputStream source, long position) {
        lockManager.writeLock(hash);
        try {
            StagingSession session = sessions.load(hash, sessionId);
            DigestBuilder digest = write(hash, session, source, position);

            sessions.save(hash, new StagingSession(sessionId, digest));
            return new StagingInfo(sessionId, digest.getHash(), digest.getLength());

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    /**
     * Terminates a content staging session. Does nothing if such a session does not exist or has expired.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     */
    public void unstageContent(Hash hash, Guid sessionId) {
        lockManager.writeLock(hash);
        try {
            sessions.release(hash, sessionId);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    private DigestBuilder computeDigest(Hash hash, long limit) throws IOException {
        Path path = stagingPath(hash);
        if (!Files.exists(path)) {
            return new DigestBuilder();
        }
        try (InputStream inputStream = Files.newInputStream(path)) {
            DigestBuilder builder = new DigestBuilder();
            copyAndDigest(new BoundedInputStream(inputStream, limit), sink(), builder);
            return builder;
        }
    }

    private DigestBuilder write(Hash hash,
                                StagingSession session,
                                InputStream source,
                                long position) throws IOException {
        DigestBuilder digest = session.getDigest();
        boolean truncate = false;

        if (digest.getHash().equals(hash)) {
            throw new StagingCompletedException();
        }
        if (position < 0 || position > digest.getLength()) {
            throw new BadRequestException("Requested position is invalid");
        }
        if (position < digest.getLength()) {
            truncate = true;
            digest = computeDigest(hash, position);
        }
        try (RandomAccessFile file = new RandomAccessFile(stagingPath(hash).toFile(), "rw")) {
            file.seek(position);
            copyAndDigest(source, new RandomAccessFileOutputStream(file), digest);
            if (truncate) {
                try (FileChannel channel = file.getChannel()) {
                    channel.truncate(digest.getLength());
                }
            }
        }
        return digest;
    }

    /**
     * Stores a new content. Fails if this content has not been previously staged.
     *
     * @param hash Content hash.
     */
    public void add(Hash hash) {
        lockManager.writeLock(hash);
        try {
            ensureStaged(hash);
            Files.move(stagingPath(hash), contentPath(hash));
            sessions.clear(hash);

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    private void ensureStaged(Hash hash) throws IOException {
        Optional<StagingSession> session = sessions.get(hash);
        DigestBuilder digest = session.isPresent() ? session.get().getDigest() : computeDigest(hash, Long.MAX_VALUE);
        if (digest.getLength() == 0) {
            throw new UnknownContentException();
        }
        if (!digest.getHash().equals(hash)) {
            throw new IntegrityCheckingFailedException();
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
        try (OutputStream target = newOutputStream(contentPath(hash))) {
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
            Files.delete(contentPath(hash));

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    /**
     * Provides staging info of a given content. If there is no current staging session for this content, returned info
     * guid is null. Additionally, if content does not even exists, returned info length is zero.
     *
     * @param hash Content hash.
     * @return Corresponding staging info.
     */
    public StagingInfo getStagingInfo(Hash hash) {
        lockManager.readLock(hash);
        try {
            Optional<StagingSession> sessionOpt = sessions.get(hash);
            if (sessionOpt.isPresent()) {
                StagingSession session = sessionOpt.get();
                DigestBuilder digest = session.getDigest();
                return new StagingInfo(session.getSessionId(), digest.getHash(), digest.getLength());
            }
            DigestBuilder digest = computeDigest(hash, Long.MAX_VALUE);
            return new StagingInfo(null, digest.getHash(), digest.getLength());

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.readUnlock(hash);
        }
    }

    /**
     * Provides an input-stream on a currently stored content.
     *
     * @param hash Content hash.
     * @param offset The position of first byte to return, inclusive.
     * @param length The amount of bytes to returns.
     * @return An input-stream on this content.
     */
    public InputStream get(Hash hash, long offset, long length) {
        checkArgument(offset >= 0, "Negative offset");
        checkArgument(length >= 0, "Negative length");
        lockManager.readLock(hash);
        try {
            InputStream inputStream = new ContentInputStream(hash, offset, length);
            inputStreams.add(inputStream);
            return inputStream;

        } catch (FileNotFoundException e) {
            lockManager.readUnlock(hash);
            throw new UnknownContentException(e);

        } catch (IOException e) {
            lockManager.readUnlock(hash);
            throw new IOFailureException(e);
        }
    }

    private Path stagingPath(Hash hash) {
        return root
                .resolve(STAGE)
                .resolve(hash.asHexadecimalString());
    }

    private Path contentPath(Hash hash) {
        return root
                .resolve(CONTENT)
                .resolve(hash.key(KEY_LENGTH))
                .resolve(hash.asHexadecimalString());
    }

    /**
     * An input-stream that supports range-reading and releases content read-lock when closed.
     */
    private class ContentInputStream extends InputStream {

        private final RandomAccessFile file;
        private final Hash hash;
        private long remaining;

        /**
         * Constructor.
         *
         * @param hash Content hash.
         * @param offset The position of first byte to return, inclusive.
         * @param length The amount of bytes to returns.
         */
        public ContentInputStream(Hash hash, long offset, long length) throws IOException {
            file = new RandomAccessFile(contentPath(hash).toFile(), "r");
            file.seek(offset);

            this.hash = hash;
            remaining = length;
        }

        @Override
        public int read() {
            if (remaining <= 0) {
                return -1;
            }
            try {
                --remaining;
                return file.read();

            } catch (IOException e) {
                throw new IOFailureException(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (remaining <= 0) {
                return -1;
            }
            try {
                int maxLen = len <= remaining ? len : (int) remaining;
                int readLen = file.read(b, off, maxLen);
                if (readLen >= 0) {
                    remaining -= readLen;
                }
                return readLen;

            } catch (IOException e) {
                throw new IOFailureException(e);
            }
        }

        @Override
        public void close() {
            try {
                file.close();

            } catch (IOException e) {
                throw new IOFailureException(e);

            } finally {
                inputStreams.remove(this);
                lockManager.readUnlock(hash);
            }
        }
    }
}
