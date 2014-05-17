package store.server.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.sleepycat.je.Transaction;
import java.io.Closeable;
import static java.util.Objects.requireNonNull;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.server.async.AsyncService;
import store.server.async.Task;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_PERIOD;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_MAX_SIZE;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_TIMEOUT;
import store.server.exception.TransactionNotFoundException;

/**
 * Retains suspended transactions, Allowing to resume them latter. Suspended transactions may be aborted and deleted
 * automatically, after an expiration delay.
 */
class TransactionCache implements Closeable {

    private final Cache<Long, SuspendedTransaction> cache;
    private final Task cleanUpTask;

    public TransactionCache(final String name, Config config, AsyncService asyncService) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(config.getInt(STORAGE_SUSPENDED_TXN_MAX_SIZE))
                .expireAfterWrite(duration(config, STORAGE_SUSPENDED_TXN_TIMEOUT),
                                  unit(config, STORAGE_SUSPENDED_TXN_TIMEOUT))
                .removalListener(new RemovalListener<Long, SuspendedTransaction>() {
            @Override
            public void onRemoval(RemovalNotification<Long, SuspendedTransaction> notification) {
                // Value can not be null as whe use strong references.
                requireNonNull(notification.getValue()).abortIfSuspended();
            }
        }).build();

        cleanUpTask = asyncService.schedule(duration(config, STORAGE_SUSPENDED_TXN_CLEANUP_PERIOD),
                                            unit(config, STORAGE_SUSPENDED_TXN_CLEANUP_PERIOD),
                                            "[" + name + "] Evicting expired transactions",
                                            new Runnable() {
            @Override
            public void run() {
                cache.cleanUp();
            }
        });
    }

    public boolean isSuspended(Transaction transaction) {
        return cache.asMap().containsKey(transaction.getId());
    }

    public void suspend(Transaction transaction) {
        requireNonNull(transaction);
        cache.put(transaction.getId(), new SuspendedTransaction(transaction));
    }

    public Transaction resume(long key) {
        SuspendedTransaction suspended = cache.getIfPresent(key);
        if (suspended != null && suspended.resume()) {
            cache.invalidate(key);
            return suspended.getTransaction();
        }
        throw new TransactionNotFoundException();
    }

    @Override
    public void close() {
        cleanUpTask.cancel();
        cache.invalidateAll();
    }
}
