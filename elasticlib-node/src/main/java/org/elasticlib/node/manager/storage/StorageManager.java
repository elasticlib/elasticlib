/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.manager.storage;

import static com.google.common.base.Preconditions.checkState;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map.Entry;
import java.util.function.Supplier;
import org.elasticlib.common.config.Config;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.value.Value;
import org.elasticlib.node.config.NodeConfig;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.task.Task;
import org.elasticlib.node.manager.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final TaskManager taskManager;
    private final Environment environment;
    private final Deque<Task> tasks = new ArrayDeque<>();
    private final Deque<Database> databases = new ArrayDeque<>();
    private final Deque<Sequence> sequences = new ArrayDeque<>();
    private final Deque<TransactionContext> txContexts = new ArrayDeque<>();
    private final ThreadLocal<TransactionContext> currentTxContext = new ThreadLocal<>();
    private boolean started = true;

    /**
     * Constructor.
     *
     * @param name Name of this storage.
     * @param path Home path of the Berkeley DB environment.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     */
    public StorageManager(String name, Path path, Config config, TaskManager taskManager) {
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
        envConfig.setExceptionListener(event -> LOG.error("error", event.getException()));

        envName = name;
        this.config = config;
        this.taskManager = taskManager;
        this.environment = new Environment(path.toFile(), envConfig);
    }

    /**
     * Stops this manager, safely releasing all its ressources.
     */
    public synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        while (!tasks.isEmpty()) {
            tasks.remove().cancel();
        }
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
    public synchronized Database openDeferredWriteDatabase(String name) {
        Database database = openDatabase(name, new DatabaseConfig()
                                         .setAllowCreate(true)
                                         .setKeyPrefixing(false)
                                         .setTransactional(false)
                                         .setDeferredWrite(true));

        if (config.getBoolean(NodeConfig.STORAGE_SYNC_ENABLED)) {
            tasks.add(taskManager.schedule(duration(config, NodeConfig.STORAGE_SYNC_INTERVAL),
                                           unit(config, NodeConfig.STORAGE_SYNC_INTERVAL),
                                           "[" + envName + "] Syncing database '" + name + "'",
                                           database::sync));
        }
        return database;
    }

    private Database openDatabase(String name, DatabaseConfig config) {
        ensureStarted();
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
        ensureStarted();
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
     * Executes supplied query in a transaction.
     *
     * @param query Query to execute.
     */
    public void inTransaction(Runnable query) {
        inTransaction(() -> {
            query.run();
            return null;
        });
    }

    /**
     * Executes supplied query in a transaction.
     *
     * @param <T> Query return type.
     * @param query Query to execute.
     * @return Query result.
     */
    public <T> T inTransaction(Supplier<T> query) {
        TransactionContext ctx = beginTransaction();
        try {
            T result = query.get();
            synchronized (this) {
                if (started) {
                    ctx.commit();
                }
            }
            return result;

        } catch (IllegalStateException e) {
            synchronized (this) {
                // Hide JE exception that may be thrown if transaction is aborted because this manager is stoppped.
                ensureStarted();
                throw e;
            }
        } finally {
            synchronized (this) {
                endTransaction(ctx);
            }
        }
    }

    private synchronized TransactionContext beginTransaction() {
        ensureStarted();
        checkState(currentTxContext.get() == null, "Nested transactions are not supported");

        TransactionContext ctx = new TransactionContext(environment.beginTransaction(null, TransactionConfig.DEFAULT));
        txContexts.add(ctx);
        currentTxContext.set(ctx);
        return ctx;
    }

    private synchronized void endTransaction(TransactionContext ctx) {
        if (started) {
            ctx.abort();
            txContexts.remove(ctx);
            currentTxContext.remove();
        }
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
        ensureStarted();
        TransactionContext ctx = currentTxContext.get();
        Cursor cursor = database.openCursor(ctx.getTransaction(), CursorConfig.READ_COMMITTED);
        ctx.add(cursor);
        return cursor;
    }

    /**
     * Provides a stream on the supplied database.
     *
     * @param <T> Database values type.
     * @param database A database.
     * @param clazz Database values class.
     * @return A stream on this database.
     */
    public <T extends Mappable> DatabaseStream<T> stream(Database database, Class<T> clazz) {
        return new DatabaseStream<>(this, database, clazz);
    }

    private void ensureStarted() {
        if (!started) {
            throw new RepositoryClosedException();
        }
    }
}
