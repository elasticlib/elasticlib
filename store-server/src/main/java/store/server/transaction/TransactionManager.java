package store.server.transaction;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import store.common.Hash;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.VolumeClosedException;
import store.server.lock.LockManager;

public class TransactionManager {

    private static final ThreadLocal<TransactionContext> currentTxContext = new ThreadLocal<>();
    private final StandaloneFileSystemConfiguration config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final LockManager lockManager = new LockManager();
    private final List<TransactionContext> txContexts = new ArrayList<>();
    private XAFileSystem filesystem;
    private boolean started;

    protected TransactionManager(Path path) {
        String dir = path.toAbsolutePath().toString();
        config = new StandaloneFileSystemConfiguration(dir, dir);
        config.setTransactionTimeout(-1);
        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stop();
            }
        });
        start();
    }

    public static TransactionManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new TransactionManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static TransactionManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        return new TransactionManager(path);
    }

    public void start() {
        lock.writeLock().lock();
        try {
            if (started) {
                return;
            }
            filesystem = XAFileSystemProxy.bootNativeXAFileSystem(config);
            filesystem.waitForBootup(-1);
            started = true;

        } catch (InterruptedException e) {
            throw new StoreRuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void stop() {
        lock.writeLock().lock();
        try {
            if (!started) {
                return;
            }
            for (TransactionContext context : txContexts) {
                context.close();
            }
            txContexts.clear();
            filesystem.shutdown();
            started = false;

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private TransactionContext createTransactionContext(boolean readOnly) {
        lock.readLock().lock();
        try {
            if (!started) {
                throw new VolumeClosedException();
            }
            Session session = filesystem.createSessionForLocalTransaction();
            TransactionContext txContext;
            if (readOnly) {
                txContext = new ReadOnlyTransactionContext(session);
            } else {
                txContext = new ReadWriteTransactionContext(session);
            }
            currentTxContext.set(txContext);
            txContexts.add(txContext);
            return txContext;

        } finally {
            lock.readLock().unlock();
        }
    }

    private void remove(TransactionContext txContext) {
        lock.readLock().lock();
        try {
            currentTxContext.remove();
            txContexts.remove(txContext);

        } finally {
            lock.readLock().unlock();
        }
    }

    public static TransactionContext currentTransactionContext() {
        TransactionContext txContext = currentTxContext.get();
        if (txContext == null) {
            throw new IllegalStateException("No current transaction");
        }
        return txContext;
    }

    public void inTransaction(Hash hash, Command command) {
        lockManager.writeLock(hash);
        try {
            TransactionContext txContext = createTransactionContext(false);
            try {
                command.apply();
                txContext.commit();

            } finally {
                txContext.close();
                remove(txContext);
            }
        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    public <T> T inTransaction(Hash hash, Query<T> query) {
        lockManager.readLock(hash);
        try {
            return inTransaction(query);

        } finally {
            lockManager.readUnlock(hash);
        }
    }

    public <T> T inTransaction(Query<T> query) {
        TransactionContext txContext = createTransactionContext(true);
        try {
            T response = query.apply();
            txContext.commit();
            return response;

        } finally {
            txContext.close();
            remove(txContext);
        }
    }
}
