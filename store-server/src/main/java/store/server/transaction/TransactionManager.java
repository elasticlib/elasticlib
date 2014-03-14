package store.server.transaction;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.WriteException;

/**
 * Manages transactions within a volume.
 */
public final class TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);
    private static final ThreadLocal<TransactionContext> CURRENT_TX_CONTEXT = new ThreadLocal<>();
    private final StandaloneFileSystemConfiguration config;
    private final Deque<TransactionContext> txContexts = new ArrayDeque<>();
    private XAFileSystem filesystem;
    private boolean started;

    private TransactionManager(Path path) {
        String dir = path.toAbsolutePath().toString();
        config = new StandaloneFileSystemConfiguration(dir, dir);
        config.setTransactionTimeout(-1);
        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    filesystem.shutdown();

                } catch (IOException e) {
                    LOG.error("Failed to shutdown XA file system", e);
                }
            }
        });
        start();
    }

    /**
     * Create a new transaction manager at specified path.
     *
     * @param path File-system path. Expected to not exists.
     * @return Created manager.
     */
    public static TransactionManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new TransactionManager(path);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Open an existing transaction manager.
     *
     * @param path Path to transaction manager home.
     * @return Opened manager
     */
    public static TransactionManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        return new TransactionManager(path);
    }

    /**
     * Starts this manager. Does nothing if it is already started.
     */
    public synchronized void start() {
        try {
            if (started) {
                return;
            }
            filesystem = XAFileSystemProxy.bootNativeXAFileSystem(config);
            filesystem.waitForBootup(-1);
            started = true;

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Stops this manager. Does nothing if it is already stopped.
     */
    public synchronized void stop() {
        try {
            if (!started) {
                return;
            }
            while (!txContexts.isEmpty()) {
                txContexts.remove().close();
            }
            filesystem.shutdown();
            started = false;

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Checks if this manager is started. Other operations will fail if this is not the case.
     *
     * @return true if this manager is started.
     */
    public synchronized boolean isStarted() {
        return started;
    }

    private synchronized TransactionContext createTransactionContext(boolean readOnly) {
        if (!started) {
            throw new RepositoryNotStartedException();
        }
        Session session = filesystem.createSessionForLocalTransaction();
        TransactionContext txContext;
        if (readOnly) {
            txContext = new ReadOnlyTransactionContext(this, session);
        } else {
            txContext = new ReadWriteTransactionContext(this, session);
        }
        CURRENT_TX_CONTEXT.set(txContext);
        txContexts.add(txContext);
        return txContext;
    }

    synchronized void remove(TransactionContext txContext) {
        CURRENT_TX_CONTEXT.remove();
        txContexts.remove(txContext);
    }

    /**
     * Provides access to the transaction context attached to current thread. Fails if no pending transaction context is
     * attached to current thread.
     *
     * @return A transaction context.
     */
    public static TransactionContext currentTransactionContext() {
        TransactionContext txContext = CURRENT_TX_CONTEXT.get();
        if (txContext == null) {
            throw new IllegalStateException("No current transaction");
        }
        return txContext;
    }

    /**
     * Execute supplied command in a read-write transaction context.
     *
     * @param command Command to execute.
     */
    public void inTransaction(Command command) {
        TransactionContext txContext = createTransactionContext(false);
        try {
            command.apply();
            txContext.commit();

        } finally {
            txContext.close();
        }
    }

    /**
     * Execute supplied query in a read-only transaction context.
     *
     * @param <T> Query return type.
     * @param query Query to execute.
     * @return Query result.
     */
    public <T> T inTransaction(Query<T> query) {
        TransactionContext txContext = createTransactionContext(true);
        try {
            return query.apply();

        } finally {
            txContext.commit();
        }
    }

    /**
     * Start a new read-only transaction.
     */
    public void beginReadOnlyTransaction() {
        createTransactionContext(true);
    }
}
