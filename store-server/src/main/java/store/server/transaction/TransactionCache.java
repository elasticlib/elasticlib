package store.server.transaction;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import static java.lang.Runtime.getRuntime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import store.server.exception.TransactionNotFoundException;

/**
 * A transaction cache. Allows to suspend a transaction, and to resume it latter. Suspended transactions may be aborted
 * and deleted automatically, after an expiration delay.
 */
class TransactionCache {

    private static final int SIZE = 20;
    private static final int TTL = 60;
    private final Cache<Long, TransactionContext> cache;

    public TransactionCache() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(SIZE)
                .expireAfterWrite(TTL, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<Long, TransactionContext>() {
            @Override
            public void onRemoval(RemovalNotification<Long, TransactionContext> notification) {
                // Value can not be null as whe use strong references.
                requireNonNull(notification.getValue()).rollbackIfSuspended();
            }
        }).build();

        final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cache.cleanUp();
            }
        }, 0, TTL, TimeUnit.SECONDS);
        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                executor.shutdown();
            }
        });
    }

    public void suspend(TransactionContext context) {
        requireNonNull(context);
        cache.put(context.getId(), context);
    }

    public TransactionContext resume(long key) {
        TransactionContext context = cache.getIfPresent(key);
        if (context != null && context.resume()) {
            cache.invalidate(key);
            return context;
        }
        throw new TransactionNotFoundException();
    }

    public void clear() {
        cache.invalidateAll();
    }
}
