package store.server.storage;

import com.google.common.base.Charsets;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.server.exception.RepositoryClosedException;

/**
 * Provides persistent storage services build atop a Berkeley DB environment.
 */
public class StorageManager {

    private static final String SEQUENCE = "sequence";
    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);
    private static final ThreadLocal<Transaction> CURRENT_TRANSACTION = new ThreadLocal<>();
    private final Environment environment;
    private final Deque<Database> databases = new ArrayDeque<>();
    private final Deque<Sequence> sequences = new ArrayDeque<>();
    private final TransactionCache cache = new TransactionCache();
    private final Deque<Transaction> transactions = new ArrayDeque<>();
    private final Deque<Cursor> cursors = new ArrayDeque<>();
    private boolean closed;

    /**
     * Constructor.
     *
     * @param name Name of this storage.
     * @param path Home path of the Berkeley DB environment.
     */
    public StorageManager(String name, Path path) {
        EnvironmentConfig config = new EnvironmentConfig()
                .setNodeName(name)
                .setSharedCache(true)
                .setTransactional(true)
                .setAllowCreate(true)
                .setLockTimeout(5, TimeUnit.MINUTES)
                .setLoggingHandler(new LoggingHandler(name));
        // Timeout is actually unexpected.

        config.setExceptionListener(new ExceptionListener() {
            @Override
            public void exceptionThrown(ExceptionEvent event) {
                LOG.error("error", event.getException());
            }
        });

        this.environment = new Environment(path.toFile(), config);
    }

    /**
     * Closes this manager, safely releasing all its ressources.
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        while (!cursors.isEmpty()) {
            safeClose(cursors.remove());
        }
        while (!transactions.isEmpty()) {
            safeAbort(transactions.remove());
        }
        safeClose(cache);
        while (!sequences.isEmpty()) {
            safeClose(sequences.remove());
        }
        while (!databases.isEmpty()) {
            safeClose(databases.remove());
        }
        safeClose(environment);
    }

    private static void safeAbort(Transaction transaction) {
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
        checkOpen();
        Database database = environment.openDatabase(null, name, new DatabaseConfig()
                .setKeyPrefixing(true)
                .setTransactional(true)
                .setAllowCreate(true));

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
        SequenceConfig config = new SequenceConfig()
                .setAllowCreate(true)
                .setInitialValue(1);

        Sequence seq = sequenceDatabase().openSequence(null, new DatabaseEntry(name.getBytes(Charsets.UTF_8)), config);
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
    public CommandResult inTransaction(Command command) {
        return inTransaction(beginTransaction(), command);
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
    public CommandResult inTransaction(long id, Command command) {
        return inTransaction(resumeTransaction(id), command);
    }

    private CommandResult inTransaction(Transaction transaction, Command command) {
        try {
            CommandResult result = command.apply();
            synchronized (this) {
                if (!closed && !cache.isSuspended(transaction)) {
                    transaction.commit();
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
                CURRENT_TRANSACTION.remove();
                if (!closed && !cache.isSuspended(transaction)) {
                    transaction.abort();
                }
            }
        }
    }

    /**
     * Executes supplied query in a transaction. Transaction is guaranteed to have been committed when query returns.
     *
     * @param <T> Query return type.
     * @param query Query to execute.
     * @return Query result.
     */
    public <T> T inTransaction(Query<T> query) {
        Transaction transaction = beginTransaction();
        try {
            return query.apply();

        } finally {
            CURRENT_TRANSACTION.remove();
            transaction.commit();
        }
    }

    /**
     * Executes supplied procedure in a transaction.
     *
     * @param procedure Procedure to execute.
     */
    public void inTransaction(Procedure procedure) {
        Transaction transaction = beginTransaction();
        try {
            procedure.apply();

        } finally {
            CURRENT_TRANSACTION.remove();
            transaction.commit();
        }
    }

    private synchronized Transaction beginTransaction() {
        checkOpen();
        Transaction transaction = environment.beginTransaction(null, TransactionConfig.DEFAULT);
        transactions.add(transaction);
        CURRENT_TRANSACTION.set(transaction);
        return transaction;
    }

    private synchronized Transaction resumeTransaction(long id) {
        checkOpen();
        Transaction transaction = cache.resume(id);
        CURRENT_TRANSACTION.set(transaction);
        return transaction;
    }

    /**
     * Suspend transaction attached to current thread. Fails if no pending transaction is attached to current thread.
     */
    public synchronized void suspendCurrentTransaction() {
        checkOpen();
        cache.suspend(CURRENT_TRANSACTION.get());
    }

    /**
     * Provides access to the transaction attached to current thread. Fails if no pending transaction is attached to
     * current thread.
     *
     * @return A transaction.
     */
    public static Transaction currentTransaction() {
        return CURRENT_TRANSACTION.get();
    }

    /**
     * Opens a cursor on supplied database.
     *
     * @param database a database.
     * @return A new cursor.
     */
    public synchronized Cursor openCursor(Database database) {
        checkOpen();
        Cursor cursor = database.openCursor(null, CursorConfig.DEFAULT);
        cursors.add(cursor);
        return cursor;
    }

    private void checkOpen() {
        if (closed) {
            throw new RepositoryClosedException();
        }
    }
}
