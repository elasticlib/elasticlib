package store.server.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.sleepycat.je.Transaction;
import java.io.Closeable;
import static java.util.Objects.requireNonNull;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import store.server.exception.TransactionNotFoundException;

class TransactionCache implements Closeable {

    private static final int SIZE = 20;
    private static final int TTL = 60;
    private final Cache<Long, SuspendedTransaction> cache;
    private final Future<?> cleanUpTask;

    public TransactionCache(ScheduledExecutorService executor) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(SIZE)
                .expireAfterWrite(TTL, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<Long, SuspendedTransaction>() {
            @Override
            public void onRemoval(RemovalNotification<Long, SuspendedTransaction> notification) {
                // Value can not be null as whe use strong references.
                requireNonNull(notification.getValue()).abortIfSuspended();
            }
        }).build();

        cleanUpTask = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cache.cleanUp();
            }
        }, 0, TTL, TimeUnit.SECONDS);
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
        cleanUpTask.cancel(false);
        cache.invalidateAll();
    }
}
