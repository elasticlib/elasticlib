package store.server.repository;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import store.common.config.Config;
import store.common.exception.BadRequestException;
import store.common.exception.IOFailureException;
import store.common.exception.InvalidRepositoryPathException;
import store.common.exception.PendingStagingSessionException;
import store.common.exception.StagingSessionNotFoundException;
import store.common.hash.Digest.DigestBuilder;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.model.StagingInfo;
import store.common.util.BoundedInputStream;
import static store.common.util.IoUtil.copyAndDigest;
import store.common.util.RandomAccessFileOutputStream;
import static store.common.util.SinkOutputStream.sink;
import store.server.config.ServerConfig;
import store.server.manager.task.Task;
import store.server.manager.task.TaskManager;

/**
 * Manages content staging.
 */
class StagingManager {

    private final Path root;
    private final LockManager lockManager;
    private final StagingSessionsCache sessions;

    private StagingManager(String name, Path root, Config config, TaskManager taskManager) {
        this.root = root;
        lockManager = new LockManager();
        sessions = new StagingSessionsCache(name, config, taskManager);
    }

    /**
     * Creates a new staging manager.
     *
     * @param name repository name.
     * @param path staging manager home.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @return Created staging manager.
     */
    public static StagingManager create(String name, Path path, Config config, TaskManager taskManager) {
        try {
            Files.createDirectory(path);
            return new StagingManager(name, path, config, taskManager);

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
    }

    /**
     * Opens an existing staging manager.
     *
     * @param name repository name.
     * @param path staging manager home.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @return Opened staging manager.
     */
    public static StagingManager open(String name, Path path, Config config, TaskManager taskManager) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        return new StagingManager(name, path, config, taskManager);
    }

    /**
     * Closes this manager.
     */
    public void close() {
        lockManager.close();
        sessions.close();
    }

    /**
     * Creates a new staging session for the content which hash is supplied.
     *
     * @param hash Hash of the content to be added latter.
     * @return Info about the session process created.
     */
    public StagingInfo stage(Hash hash) {
        lockManager.writeLock(hash);
        try {
            if (sessions.containsKey(hash)) {
                throw new PendingStagingSessionException();
            }
            Guid sessionId = Guid.random();
            DigestBuilder digest = computeDigest(hash, Long.MAX_VALUE);

            sessions.save(hash, new StagingSession(sessionId, digest));
            return new StagingInfo(sessionId, digest.getHash(), digest.getLength());

        } catch (IOException e) {
            throw new IOFailureException(e);

        } finally {
            lockManager.writeUnlock(hash);
        }
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
    public StagingInfo write(Hash hash, Guid sessionId, InputStream source, long position) {
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

    private DigestBuilder computeDigest(Hash hash, long limit) throws IOException {
        Path path = path(hash);
        if (!Files.exists(path)) {
            return new DigestBuilder();
        }
        try (InputStream inputStream = Files.newInputStream(path)) {
            DigestBuilder builder = new DigestBuilder();
            copyAndDigest(new BoundedInputStream(inputStream, limit), sink(), builder);
            return builder;
        }
    }

    private DigestBuilder write(Hash hash, StagingSession session, InputStream source, long position) throws IOException {
        DigestBuilder digest = session.getDigest();
        boolean truncate = false;

        if (position < 0 || position > digest.getLength()) {
            throw new BadRequestException("Requested position is invalid");
        }
        if (position < digest.getLength()) {
            truncate = true;
            digest = computeDigest(hash, position);
        }
        try (RandomAccessFile file = new RandomAccessFile(path(hash).toFile(), "rw")) {
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

    private Path path(Hash hash) {
        return root.resolve(hash.asHexadecimalString());
    }

    /**
     * Staging session.
     */
    private static class StagingSession {

        private final Guid sessionId;
        private final DigestBuilder digest;

        /**
         * Constructor.
         *
         * @param sessionId Staging session identifier.
         * @param digest Current digest.
         */
        public StagingSession(Guid sessionId, DigestBuilder digest) {
            this.sessionId = sessionId;
            this.digest = digest;
        }

        /**
         * @return The identifier of the session this context is associated with.
         */
        public Guid getSessionId() {
            return sessionId;
        }

        /**
         * @return Current digest of this context.
         */
        public DigestBuilder getDigest() {
            return digest;
        }
    }

    /**
     * Memory cache of the pending staging sessions.
     */
    private static class StagingSessionsCache implements Closeable {

        // TODO put in config !
        private static final int STAGING_MAX_SIZE = 100;
        private static final int STAGING_TIMEOUT = 60;
        private static final int STAGING_CLEANUP_INTERVAL = 30;
        private final Cache<Hash, StagingSession> cache;
        private final Task cleanUpTask;

        /**
         * Constructor.
         *
         * @param name repository name.
         * @param config Configuration holder.
         * @param taskManager Asynchronous tasks manager.
         */
        public StagingSessionsCache(String name, Config config, TaskManager taskManager) {
            cache = CacheBuilder.newBuilder()
                    .maximumSize(STAGING_MAX_SIZE)
                    .expireAfterWrite(STAGING_TIMEOUT, TimeUnit.SECONDS)
                    .build();

            if (config.getBoolean(ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_ENABLED)) {
                cleanUpTask = taskManager
                        .schedule(STAGING_CLEANUP_INTERVAL, TimeUnit.SECONDS,
                                  "[" + name + "] Evicting expired staging sessions",
                                  new Runnable() {
                    @Override
                    public void run() {
                        cache.cleanUp();
                    }
                });
            } else {
                cleanUpTask = null;
            }
        }

        /**
         * Checks existence of a given session.
         *
         * @param hash A content hash.
         * @return True if this cache contains a session associated with supplied hash.
         */
        public boolean containsKey(Hash hash) {
            return cache.asMap().containsKey(hash);
        }

        /**
         * Save supplied session, for latter retrieval with supplied hash.
         *
         * @param hash A content hash.
         * @param session Session to save.
         */
        public void save(Hash hash, StagingSession session) {
            cache.put(hash, session);
        }

        /**
         * Loads session associated with supplied hash. Fails if it does not exist or does not match supplied ID.
         *
         * @param hash A content hash.
         * @param sessionId Expected session identifier.
         * @return Associated session.
         */
        public StagingSession load(Hash hash, Guid sessionId) {
            StagingSession session = cache.getIfPresent(hash);
            if (session == null || !session.getSessionId().equals(sessionId)) {
                throw new StagingSessionNotFoundException();
            }
            cache.invalidate(hash);
            return session;
        }

        @Override
        public void close() {
            if (cleanUpTask != null) {
                cleanUpTask.cancel();
            }
            cache.invalidateAll();
        }
    }
}
