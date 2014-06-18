package store.server.storage;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.common.value.Value;
import store.server.async.AsyncService;
import store.server.async.Task;
import static store.server.config.ServerConfig.STORAGE_SYNC_PERIOD;
import store.server.exception.RepositoryClosedException;
import static store.server.storage.DatabaseEntries.entry;

/**
 * Provides persistent storage services build atop a Berkeley DB environment.
 * <p>
 * Tracks all opened handles (Databases, Sequences, Transactions...). When this manager is closed, it aborts all
 * transactions and closes all tracked handles in proper order before closing Berkeley DB environment. Therefore, it can
 * safely be closed at any moment.
 * <p>
 * Adds the ability to suspend a transaction before leaving the transaction scope and to resume it latter in another
 * transaction scope. Note that suspended transactions are automatically evicted if they are not resumed after a given
 * expiration delay.
 * <p>
 * This class is thread-safe and can be safely accessed by multiple concurrent threads.
 */
public class StorageManager {

    private static final String JE = "je";
    private static final String SEQUENCE = "sequence";
    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);
    private final String envName;
    private final Config config;
    private final AsyncService asyncService;
    private final Environment environment;
    private final TransactionCache cache;
    private final Deque<Task> tasks = new ArrayDeque<>();
    private final Deque<Database> databases = new ArrayDeque<>();
    private final Deque<Sequence> sequences = new ArrayDeque<>();
    private final Deque<TransactionContext> txContexts = new ArrayDeque<>();
    private final ThreadLocal<TransactionContext> currentTxContext = new ThreadLocal<>();
    private boolean closed;

    /**
     * Constructor.
     *
     * @param name Name of this storage.
     * @param path Home path of the Berkeley DB environment.
     * @param config Configuration holder.
     * @param asyncService Async service.
     */
    public StorageManager(String name, Path path, Config config, AsyncService asyncService) {
        EnvironmentConfig envConfig = new EnvironmentConfig()
                .setNodeName(name)
                .setSharedCache(true)
                .setTransactional(true)
                .setAllowCreate(true)
                .setLoggingHandler(new LoggingHandler(name));

        if (config.containsKey(JE)) {
            for (Entry<String, Value> entry : config.getFlatMap(JE).entrySet()) {
                envConfig.setConfigParam(entry.getKey(), entry.getValue().toString());
            }
        }
        envConfig.setExceptionListener(new ExceptionListener() {
            @Override
            public void exceptionThrown(ExceptionEvent event) {
                LOG.error("error", event.getException());
            }
        });

        envName = name;
        this.config = config;
        this.asyncService = asyncService;
        this.environment = new Environment(path.toFile(), envConfig);
        this.cache = new TransactionCache(envName, config, asyncService);
    }

    /**
     * Closes this manager, safely releasing all its ressources.
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        while (!tasks.isEmpty()) {
            tasks.remove().cancel();
        }
        safeClose(cache);
        while (!txContexts.isEmpty()) {
            safeAbort(txContexts.remove());
        }
        while (!sequences.isEmpty()) {
            safeClose(sequences.remove());
        }
        while (!databases.isEmpty()) {
            safeClose(databases.remove());
        }
        safeClose(environment);
    }

    private static void safeAbort(TransactionContext transaction) {
        try {
            transaction.abort();

        } catch (Exception e) {
            LOG.error("Failed to abort transaction", e);
        }
    }

    private static void safeClose(Closeable closeable) {
        try {
            closeable.close();

        } catch (Exception e) {
            LOG.error("Failed to close handle", e);
        }
    }

    /**
     * Opens a database with supplied name. If this database does not exist, it is created.
     *
     * @param name Database name.
     * @return Corresponding database handle.
     */
    public synchronized Database openDatabase(String name) {
        return openDatabase(name, new DatabaseConfig()
                .setAllowCreate(true)
                .setKeyPrefixing(true)
                .setTransactional(true));
    }

    /**
     * Opens a database with supplied name. If this database does not exist, it is created. Returned database operates
     * in deffered-write mode. It does not support transactions. All changes performed to this database are periodically
     * written behind the scenes.
     *
     * @param name Database name.
     * @return Corresponding database handle.
     */
    public synchronized Database openDeferredWriteDatabase(final String name) {
        final Database database = openDatabase(name, new DatabaseConfig()
                .setAllowCreate(true)
                .setKeyPrefixing(false)
                .setTransactional(false)
                .setDeferredWrite(true));

        tasks.add(asyncService.schedule(duration(config, STORAGE_SYNC_PERIOD), unit(config, STORAGE_SYNC_PERIOD),
                                        "[" + envName + "] Syncing database '" + name + "'",
                                        new Runnable() {
            @Override
            public void run() {
                database.sync();
            }
        }));

        return database;
    }

    private Database openDatabase(String name, DatabaseConfig config) {
        ensureOpen();
        Database database = environment.openDatabase(null, name, config);
        databases.add(database);
        return database;
    }

    /**
     * Opens a sequence with supplied name. If this sequence does not exist, it is created.
     *
     * @param name Sequence name.
     * @return Corresponding sequence handle.
     */
    public synchronized Sequence openSequence(String name) {
        ensureOpen();
        SequenceConfig seqConfig = new SequenceConfig()
                .setAllowCreate(true)
                .setInitialValue(1);

        Sequence seq = sequenceDatabase().openSequence(null, entry(name), seqConfig);
        sequences.add(seq);
        return seq;
    }

    private Database sequenceDatabase() {
        for (Database database : databases) {
            if (database.getDatabaseName().equals(SEQUENCE)) {
                return database;
            }
        }
        Database database = environment.openDatabase(null, SEQUENCE, new DatabaseConfig().setAllowCreate(true));
        databases.add(database);
        return database;
    }

    /**
     * Executes supplied command in a transaction. Transaction is guaranteed to have been committed or aborted when
     * command returns, unless it is suspended during command execution.
     *
     * @param command Command to execute.
     * @return Supplied command invocation result.
     */
    public CommandResult inTransaction(final Command command) {
        return inTransaction(beginTransaction(), new Query<CommandResult>() {
            @Override
            public CommandResult apply() {
                return command.apply();
            }
        });
    }

    /**
     * Retrieves and resumes suspended transaction associated to supplied identifier. Then executes supplied command
     * within this transaction. Fails if there is no suspended transaction associated to this identifier, or if such
     * transaction has expired.
     *
     * @param id Previously suspended transaction identifier.
     * @param command Command to execute.
     * @return Supplied command invocation result.
     */
    public CommandResult inTransaction(long id, final Command command) {
        return inTransaction(resumeTransaction(id), new Query<CommandResult>() {
            @Override
            public CommandResult apply() {
                return command.apply();
            }
        });
    }

    /**
     * Executes supplied query in a transaction.
     *
     * @param <T> Query return type.
     * @param query Query to execute.
     * @return Query result.
     */
    public <T> T inTransaction(Query<T> query) {
        return inTransaction(beginTransaction(), query);
    }

    /**
     * Executes supplied procedure in a transaction.
     *
     * @param procedure Procedure to execute.
     */
    public void inTransaction(final Procedure procedure) {
        inTransaction(beginTransaction(), new Query<Void>() {
            @Override
            public Void apply() {
                procedure.apply();
                return null;
            }
        });
    }

    private <T> T inTransaction(TransactionContext ctx, Query<T> query) {
        try {
            T result = query.apply();
            synchronized (this) {
                if (!closed) {
                    ctx.commitIfRunning();
                }
            }
            return result;

        } catch (IllegalStateException e) {
            synchronized (this) {
                // Hide JE exception that may be thrown if transaction is aborted because this manager is closed.
                ensureOpen();
                throw e;
            }
        } finally {
            synchronized (this) {
                if (!closed) {
                    ctx.abortIfRunning();
                    txContexts.remove(ctx);
                    currentTxContext.remove();
                }
            }
        }
    }

    private synchronized TransactionContext beginTransaction() {
        ensureOpen();
        TransactionContext ctx = new TransactionContext(environment.beginTransaction(null, TransactionConfig.DEFAULT));
        txContexts.add(ctx);
        currentTxContext.set(ctx);
        return ctx;
    }

    private synchronized TransactionContext resumeTransaction(long id) {
        ensureOpen();
        TransactionContext ctx = cache.resume(id);
        txContexts.add(ctx);
        currentTxContext.set(ctx);
        return ctx;
    }

    /**
     * Suspends the transaction attached to current thread. Fails if no pending transaction is attached to current
     * thread.
     * <p>
     * Note that any further attempt to get current transaction will fail. Caller is expected to resume this transaction
     * in a new transactional block if he wants to performs other operations and to commit all pending changes.
     */
    public synchronized void suspendCurrentTransaction() {
        ensureOpen();
        cache.suspend(currentTxContext.get());

        // This is redundant, because current context is removed when leaving the current transaction block.
        // However, it ensures that current transaction is no longer available. And ThreadLocal.remove() is indempotent.
        currentTxContext.remove();
    }

    /**
     * Provides access to the transaction attached to current thread. Fails if no pending transaction is attached to
     * current thread.
     *
     * @return A transaction.
     */
    public Transaction currentTransaction() {
        return currentTxContext.get().getTransaction();
    }

    /**
     * Opens a cursor on supplied database.
     * <p>
     * Returned cursor is protected by the current transaction and uses committed-read isolation. This means that locks
     * held by this cursor on a given record are released as soon as the cursor moves or is closed. By default, JE
     * cursors use repeatable-read isolation and hold locks until their encompassing transaction is closed, even if they
     * are themselve closed !
     * <p>
     * Using committed-read isolation is perfectly fine as long as the cursor moves in a single direction. Furthermore,
     * as soon as the cursor is done with a given record, encompassing transaction can modify this record without
     * dead-locking.
     *
     * @param database a database.
     * @return A new cursor.
     */
    public synchronized Cursor openCursor(Database database) {
        ensureOpen();
        TransactionContext ctx = currentTxContext.get();
        Cursor cursor = database.openCursor(ctx.getTransaction(), CursorConfig.READ_COMMITTED);
        ctx.add(cursor);
        return cursor;
    }

    private void ensureOpen() {
        if (closed) {
            throw new RepositoryClosedException();
        }
    }
}
