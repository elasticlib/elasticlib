package store.server.transaction;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import store.common.CommandResult;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.WriteException;
import static store.server.transaction.TransactionContext.newTransactionContext;

/**
 * Manages transactions within a volume.
 */
public final class TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);
    private final StandaloneFileSystemConfiguration config;
    private final TransactionCache cache = new TransactionCache();
    private final Deque<TransactionContext> txContexts = new ConcurrentLinkedDeque<>();
    private XAFileSystem filesystem;
    private boolean started;
    private int nextId;

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
     * Creates a new transaction manager at specified path.
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
     * Opens an existing transaction manager.
     *
     * @param path Path to transaction manager home.
     * @return Opened manager.
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
                txContexts.remove().rollback();
            }
            cache.clear();
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
        TransactionContext txContext = newTransactionContext(this, session, readOnly, ++nextId);
        txContexts.add(txContext);
        return txContext;
    }

    // This method is not synchronized in order to avoid deadlocking if both TransactionManager.stop()
    // and TransactionContext.close() are called concurrently.
    void remove(TransactionContext txContext) {
        txContexts.remove(txContext);
    }

    void suspend(TransactionContext context) {
        cache.suspend(context);
    }

    /**
     * Execute supplied command in a read-write transaction. Transaction is guaranteed to have been committed or
     * roll-backed when command returns, unless it is suspended during command execution.
     *
     * @param command Command to execute.
     * @return Supplied command invocation result.
     */
    public CommandResult inTransaction(Command command) {
        return inTransaction(createTransactionContext(false), command);
    }

    /**
     * Retrieves and resumes suspended transaction associated to supplied key. Then executes supplied command within
     * this transaction. Fails if there is no suspended transaction associated to this key, or if such transaction has
     * expired.
     *
     * @param key The key suspended transaction has previously been associated to.
     * @param command Command to execute.
     * @return Supplied command invocation result.
     */
    public CommandResult inTransaction(int key, Command command) {
        return inTransaction(cache.resume(key), command);
    }

    private CommandResult inTransaction(TransactionContext txContext, Command command) {
        try {
            CommandResult result = command.apply();
            txContext.commitAndDetachIfNotSuspended();
            return result;

        } finally {
            txContext.rollbackAndDetachIfNotSuspended();
        }
    }

    /**
     * Execute supplied query in a read-only transaction. Transaction is guaranteed to have been committed when query
     * returns, unless query successfully opens and returns a committing input.
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
            txContext.commitAndDetachIfNotDetached();
        }
    }
}
