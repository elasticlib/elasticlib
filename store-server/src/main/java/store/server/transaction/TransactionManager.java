package store.server.transaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ConcurrentModificationException;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import store.common.Hash;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.lock.LockManager;

public class TransactionManager {

    private static final ThreadLocal<TransactionContext> txContexts = new ThreadLocal<>();
    private final LockManager lockManager = new LockManager();
    private final XAFileSystem filesystem;

    private TransactionManager(Path path) {
        String dir = path.toAbsolutePath().toString();
        StandaloneFileSystemConfiguration config = new StandaloneFileSystemConfiguration(dir, "instance");
        filesystem = XAFileSystemProxy.bootNativeXAFileSystem(config);
        try {
            filesystem.waitForBootup(-1);

        } catch (InterruptedException e) {
            throw new StoreRuntimeException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    filesystem.shutdown();

                } catch (IOException e) {
                    throw new StoreRuntimeException(e);
                }
            }
        });
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

    public static TransactionContext currentTransactionContext() {
        TransactionContext txContext = txContexts.get();
        if (txContext == null) {
            throw new IllegalStateException("No current transaction");
        }
        return txContext;
    }

    public void inTransaction(Hash hash, Command command) {
        if (!lockManager.writeLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            Session session = filesystem.createSessionForLocalTransaction();
            txContexts.set(new ReadWriteTransactionContext(session));
            try {
                command.apply();
                session.commit();

            } catch (Throwable e) {
                session.rollback();
                throw e;
            }
        } catch (NoTransactionAssociatedException e) {
            throw new RuntimeException(e);

        } finally {
            txContexts.remove();
            lockManager.writeUnlock(hash);
        }
    }

    public <T> T inTransaction(Hash hash, Query<T> query) {
        if (!lockManager.readLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            return inTransaction(query);

        } finally {
            lockManager.readUnlock(hash);
        }
    }

    public <T> T inTransaction(Query<T> query) {
        try {
            Session session = filesystem.createSessionForLocalTransaction();
            txContexts.set(new ReadOnlyTransactionContext(session));
            try {
                T response = query.apply();
                session.commit();
                return response;

            } catch (Throwable e) {
                session.rollback();
                throw e;
            }
        } catch (NoTransactionAssociatedException e) {
            throw new StoreRuntimeException(e);

        } finally {
            txContexts.remove();
        }
    }
}
