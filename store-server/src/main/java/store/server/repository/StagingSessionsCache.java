package store.server.repository;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Closeable;
import java.util.Optional;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.common.exception.StagingSessionNotFoundException;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.server.config.ServerConfig;
import store.server.manager.task.Task;
import store.server.manager.task.TaskManager;

/**
 * Memory cache for content staging sessions in progress.
 */
class StagingSessionsCache implements Closeable {

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
                .maximumSize(config.getInt(ServerConfig.STAGING_SESSIONS_MAX_SIZE))
                .expireAfterWrite(duration(config, ServerConfig.STAGING_SESSIONS_TIMEOUT),
                                  unit(config, ServerConfig.STAGING_SESSIONS_TIMEOUT))
                .build();

        if (config.getBoolean(ServerConfig.STAGING_SESSIONS_CLEANUP_ENABLED)) {
            cleanUpTask = taskManager
                    .schedule(duration(config, ServerConfig.STAGING_SESSIONS_CLEANUP_INTERVAL),
                              unit(config, ServerConfig.STAGING_SESSIONS_CLEANUP_INTERVAL),
                              "[" + name + "] Evicting expired staging sessions",
                              () -> cache.cleanUp());
        } else {
            cleanUpTask = null;
        }
    }

    /**
     * Saves supplied session, for latter retrieval with supplied hash.
     *
     * @param hash A content hash.
     * @param session Session to save.
     */
    public void save(Hash hash, StagingSession session) {
        cache.put(hash, session);
    }

    /**
     * Loads session associated with supplied hash. Fails if it does not exist or does not match supplied session
     * identifier.
     *
     * @param hash A content hash.
     * @param sessionId Expected session identifier.
     * @return Associated session.
     */
    public StagingSession load(Hash hash, Guid sessionId) {
        StagingSession session = cache.getIfPresent(hash);
        if (session != null && session.getSessionId() != null && session.getSessionId().equals(sessionId)) {
            cache.invalidate(hash);
            return session;
        }
        throw new StagingSessionNotFoundException();
    }

    /**
     * Provides session associated with supplied hash, if any.
     *
     * @param hash A content hash.
     * @return Associated session, if any.
     */
    public Optional<StagingSession> get(Hash hash) {
        return Optional.ofNullable(cache.getIfPresent(hash));
    }

    /**
     * Deletes session associated with supplied hash, if any.
     *
     * @param hash A content hash.
     */
    public void clear(Hash hash) {
        cache.invalidate(hash);
    }

    /**
     * Releases session associated with supplied hash, if it exists and matches supplied session identifier. Associated
     * digest is temporary kept in cache.
     *
     * @param hash A content hash.
     * @param sessionId Expected session identifier.
     */
    public void release(Hash hash, Guid sessionId) {
        StagingSession session = cache.getIfPresent(hash);
        if (session != null && session.getSessionId() != null && session.getSessionId().equals(sessionId)) {
            cache.put(hash, new StagingSession(null, session.getDigest()));
        }
    }

    @Override
    public void close() {
        if (cleanUpTask != null) {
            cleanUpTask.cancel();
        }
        cache.invalidateAll();
    }
}
