package store.server.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.io.Closeable;
import static java.util.Objects.requireNonNull;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.server.async.AsyncManager;
import store.server.async.Task;
import store.server.config.ServerConfig;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_MAX_SIZE;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_TIMEOUT;
import store.server.exception.TransactionNotFoundException;

/**
 * Retains suspended transactions, allowing to resume them latter. Suspended transactions may be aborted and deleted
 * automatically, after an expiration delay.
 */
class TransactionCache implements Closeable {

    private final Cache<Long, TransactionContext> cache;
    private final Task cleanUpTask;

    public TransactionCache(final String name, Config config, AsyncManager asyncManager) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(config.getInt(STORAGE_SUSPENDED_TXN_MAX_SIZE))
                .expireAfterWrite(duration(config, STORAGE_SUSPENDED_TXN_TIMEOUT),
                                  unit(config, STORAGE_SUSPENDED_TXN_TIMEOUT))
                .removalListener(new RemovalListener<Long, TransactionContext>() {
            @Override
            public void onRemoval(RemovalNotification<Long, TransactionContext> notification) {
                // Value can not be null as whe use strong references.
                requireNonNull(notification.getValue()).abortIfSuspended();
            }
        }).build();

        if (config.getBoolean(ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_ENABLED)) {
            cleanUpTask = asyncManager.schedule(duration(config, ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_INTERVAL),
                                                unit(config, ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_INTERVAL),
                                                "[" + name + "] Evicting expired transactions",
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

    public void suspend(TransactionContext ctx) {
        requireNonNull(ctx);
        ctx.suspend();
        cache.put(ctx.getId(), ctx);
    }

    public TransactionContext resume(long key) {
        TransactionContext ctx = cache.getIfPresent(key);
        if (ctx != null && ctx.resume()) {
            cache.invalidate(key);
            return ctx;
        }
        throw new TransactionNotFoundException();
    }

    @Override
    public void close() {
        if (cleanUpTask != null) {
            cleanUpTask.cancel();
        }
        cache.invalidateAll();
    }
}
