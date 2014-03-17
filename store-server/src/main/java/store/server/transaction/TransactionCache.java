package store.server.transaction;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import static java.lang.Runtime.getRuntime;
import static java.util.Objects.requireNonNull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import store.server.exception.TransactionNotFoundException;

/**
 * A transaction cache. Allows to suspend a transaction, and to resume it latter. Suspended transactions may be aborted
 * and deleted automatically, after an expiration delay.
 */
class TransactionCache {

    private static final int SIZE = 20;
    private static final int TTL = 60;
    private final AtomicInteger nextKey;
    private final Cache<Integer, CachedContext> cache;

    public TransactionCache() {
        nextKey = new AtomicInteger();
        cache = CacheBuilder.newBuilder()
                .maximumSize(SIZE)
                .expireAfterWrite(TTL, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<Integer, CachedContext>() {
            @Override
            public void onRemoval(RemovalNotification<Integer, CachedContext> notification) {
                // Value can not be null as whe use strong references.
                CachedContext cachedContext = requireNonNull(notification.getValue());
                if (cachedContext.remove()) {
                    cachedContext.get().close(false, false);
                }
            }
        }).build();

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
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

    public int suspend(TransactionContext context) {
        requireNonNull(context);
        int key = nextKey.incrementAndGet();
        cache.put(key, new CachedContext(context));
        return key;
    }

    public TransactionContext resume(int key) {
        CachedContext context = cache.getIfPresent(key);
        if (context != null && context.resume()) {
            cache.invalidate(key);
            return context.get();
        }
        throw new TransactionNotFoundException();
    }

    public void clear() {
        cache.invalidateAll();
    }

    private static class CachedContext {

        private final TransactionContext context;
        private AtomicReference<State> state;

        public CachedContext(TransactionContext context) {
            this.context = context;
            state = new AtomicReference<>(State.SUSPENDED);
        }

        public boolean resume() {
            return state.compareAndSet(State.SUSPENDED, State.RESUMED);
        }

        public boolean remove() {
            return state.compareAndSet(State.SUSPENDED, State.REMOVED);
        }

        public TransactionContext get() {
            return context;
        }
    }

    private static enum State {

        SUSPENDED,
        RESUMED,
        REMOVED
    }
}
