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
    /**
     * We maintains a stack of contextes per threads. This allows to nest transactions, even against differents storage
     * managers, without conflicting.
     */
    private static final ThreadLocal<Deque<TransactionContext>> CURRENT_TX_CONTEXT =
            new ThreadLocal<Deque<TransactionContext>>() {
        @Override
        protected Deque<TransactionContext> initialValue() {
            return new ArrayDeque<>();
        }
    };
    private final String envName;
    private final Config config;
    private final AsyncService asyncService;
    private final Environment environment;
    private final Deque<Database> databases = new ArrayDeque<>();
    private final Deque<Sequence> sequences = new ArrayDeque<>();
    private final TransactionCache cache;
    private final Deque<TransactionContext> txContexts = new ArrayDeque<>();
    private final Deque<Task> tasks = new ArrayDeque<>();
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
     * Opens a database with supplied name. If this database does not exist, it is created.
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
        checkOpen();
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
        checkOpen();
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
     * Executes supplied query in a transaction. Transaction is guaranteed to have been committed when query returns.
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
                checkOpen();
                throw e;
            }
        } finally {
            synchronized (this) {
                if (!closed) {
                    ctx.abortIfRunning();
                    txContexts.remove(ctx);
                    CURRENT_TX_CONTEXT.get().pop();
                }
            }
        }
    }

    private synchronized TransactionContext beginTransaction() {
        checkOpen();
        TransactionContext ctx = new TransactionContext(environment.beginTransaction(null, TransactionConfig.DEFAULT));
        txContexts.add(ctx);
        CURRENT_TX_CONTEXT.get().push(ctx);
        return ctx;
    }

    private synchronized TransactionContext resumeTransaction(long id) {
        checkOpen();
        TransactionContext ctx = cache.resume(id);
        txContexts.add(ctx);
        CURRENT_TX_CONTEXT.get().push(ctx);
        return ctx;
    }

    /**
     * Suspend transaction attached to current thread. Fails if no pending transaction is attached to current thread.
     */
    public synchronized void suspendCurrentTransaction() {
        checkOpen();
        // Does not pop the context here, it will be removed when leaving the current transaction block.
        cache.suspend(CURRENT_TX_CONTEXT.get().peek());
    }

    /**
     * Provides access to the transaction attached to current thread. Fails if no pending transaction is attached to
     * current thread.
     *
     * @return A transaction.
     */
    public static Transaction currentTransaction() {
        return CURRENT_TX_CONTEXT.get().peek().getTransaction();
    }

    /**
     * Opens a cursor on supplied database.
     * <p>
     * Returned cursor is protected by the current transaction and uses committed read isolation. This means that locks
     * held by this cursor on a given record are released as soon as the cursor moves or is closed. By default, JE
     * cursors use repeatable read isolation and hold locks until their encompassing transaction is closed, even if they
     * are themselve closed !
     * <p>
     * Using committed read isolation is perfectly fine as long as the cursor moves in a single direction. Furthermore,
     * as soon as the cursor is done with a given record, encompassing transaction can modify this record without
     * dead-locking.
     *
     * @param database a database.
     * @return A new cursor.
     */
    public synchronized Cursor openCursor(Database database) {
        checkOpen();
        TransactionContext ctx = CURRENT_TX_CONTEXT.get().peek();
        Cursor cursor = database.openCursor(ctx.getTransaction(), CursorConfig.READ_COMMITTED);
        ctx.add(cursor);
        return cursor;
    }

    private void checkOpen() {
        if (closed) {
            throw new RepositoryClosedException();
        }
    }
}
