package store.server.transaction;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import org.xadisk.filesystem.exceptions.XAApplicationException;
import store.server.exception.RepositoryNotStartedException;

/**
 * A transaction context.
 * <p>
 * Provides access to the file-system in a transactional fashion : all file operations in a transaction are executed
 * atomically and in isolation from other operations in concurrent transactions. Once a transaction is commited, all
 * modifications are durably written on disk and become visible.
 * <p>
 * A transaction may be read-write or read-only :<br>
 * - Operations in read-write transactions are done with exclusive locking, whereas read-only transactions use shared
 * locking.<br>
 * - All mutative operations require a read-write transaction.
 */
public abstract class TransactionContext {

    private static final ThreadLocal<TransactionContext> CURRENT_TX_CONTEXT = new ThreadLocal<>();
    private final Lock lock = new ReentrantLock();
    private final TransactionManager transactionManager;
    private final Session session;
    private final boolean lockExclusively;
    private final int id;
    private boolean suspended = false;
    private boolean detached = false;
    private boolean closed = false;

    TransactionContext(TransactionManager transactionManager, Session session, boolean lockExclusively, int id) {
        this.transactionManager = transactionManager;
        this.session = session;
        this.lockExclusively = lockExclusively;
        this.id = id;
    }

    static TransactionContext newTransactionContext(TransactionManager transactionManager,
                                                    Session session,
                                                    boolean readOnly,
                                                    int id) {
        TransactionContext txContext;
        if (readOnly) {
            txContext = new ReadOnlyTransactionContext(transactionManager, session, id);
        } else {
            txContext = new ReadWriteTransactionContext(transactionManager, session, id);
        }
        CURRENT_TX_CONTEXT.set(txContext);
        return txContext;
    }

    /**
     * Provides access to the transaction context attached to current thread. Fails if no pending transaction context is
     * attached to current thread.
     *
     * @return A transaction context.
     */
    public static TransactionContext current() {
        TransactionContext txContext = CURRENT_TX_CONTEXT.get();
        if (txContext == null) {
            throw new IllegalStateException("No current transaction");
        }
        return txContext;
    }

    void commit() {
        close(true, false, Predicates.<TransactionContext>alwaysTrue());
    }

    void rollback() {
        close(false, false, Predicates.<TransactionContext>alwaysTrue());
    }

    void rollbackIfSuspended() {
        close(false, false, new Predicate<TransactionContext>() {
            @Override
            public boolean apply(TransactionContext txContext) {
                return txContext.suspended;
            }
        });
    }

    void commitAndDetachIfNotDetached() {
        close(true, true, new Predicate<TransactionContext>() {
            @Override
            public boolean apply(TransactionContext txContext) {
                return !txContext.detached;
            }
        });
    }

    void commitAndDetachIfNotSuspended() {
        close(true, true, new Predicate<TransactionContext>() {
            @Override
            public boolean apply(TransactionContext txContext) {
                return !txContext.suspended;
            }
        });
    }

    void rollbackAndDetachIfNotSuspended() {
        close(false, true, new Predicate<TransactionContext>() {
            @Override
            public boolean apply(TransactionContext txContext) {
                return !txContext.suspended;
            }
        });
    }

    private void close(boolean commit, boolean detachFromCurrentThread, Predicate<TransactionContext> predicate) {
        lock.lock();
        try {
            if (closed || !predicate.apply(this)) {
                return;
            }
            closed = true;
            if (detachFromCurrentThread) {
                detach();
            }
            if (commit) {
                session.commit();
            } else {
                session.rollback();
            }
            transactionManager.remove(this);

        } catch (NoTransactionAssociatedException e) {
            throw new AssertionError(e);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Suspend this transaction.
     */
    public void suspend() {
        lock.lock();
        try {
            if (closed) {
                throw new RepositoryNotStartedException();
            }
            if (suspended) {
                throw new IllegalStateException();
            }
            suspended = true;
            CURRENT_TX_CONTEXT.remove();
            transactionManager.suspend(this);

        } finally {
            lock.unlock();
        }
    }

    boolean resume() {
        lock.lock();
        try {
            if (closed) {
                return false;
            }
            suspended = false;
            CURRENT_TX_CONTEXT.set(this);
            return true;

        } finally {
            lock.unlock();
        }
    }

    private void detach() {
        detached = true;
        CURRENT_TX_CONTEXT.remove();
    }

    /**
     * Provides this transaction identifier. Each identifier uniquely identifies a transaction for a given transaction
     * manager during the lifetime of the latter. Please note that identifiers are not persisted and are reused accross
     * application restarts.
     *
     * @return This transaction identifier.
     */
    public int getId() {
        return id;
    }

    <T> T inLock(TransactionFunction<T> function) {
        lock.lock();
        try {
            if (closed) {
                throw new RepositoryNotStartedException();
            }
            return function.apply();

        } catch (Exception e) {
            throw new IllegalStateException(e);

        } finally {
            lock.unlock();
        }
    }

    void inLock(final TransactionProcedure action) {
        inLock(new TransactionFunction<Void>() {
            @Override
            public Void apply() throws Exception {
                action.apply();
                return null;
            }
        });
    }

    interface TransactionFunction<T> {

        T apply() throws Exception;
    }

    interface TransactionProcedure {

        void apply() throws Exception;
    }

    /**
     * Checks if a file exists at supplied path.
     *
     * @param path A file-system path.
     * @return true if this is the case.
     */
    public final boolean exists(final Path path) {
        return inLock(new TransactionFunction<Boolean>() {
            @Override
            public Boolean apply() throws XAApplicationException, InterruptedException {
                return session.fileExists(path.toFile(), lockExclusively);
            }
        });
    }

    /**
     * Lists file names at supplied path. Fails if file at supplied path does not exists or is not a directory.
     *
     * @param path A file-system path.
     * @return An array of file names.
     */
    public final String[] listFiles(final Path path) {
        return inLock(new TransactionFunction<String[]>() {
            @Override
            public String[] apply() throws XAApplicationException, InterruptedException {
                return session.listFiles(path.toFile(), lockExclusively);
            }
        });
    }

    /**
     * Provides length of file at supplied path. Fails if file at supplied path does not exists or is not a regular one.
     *
     * @param path A file-system path.
     * @return A length in bytes.
     */
    public final long fileLength(final Path path) {
        return inLock(new TransactionFunction<Long>() {
            @Override
            public Long apply() throws XAApplicationException, InterruptedException {
                return session.getFileLength(path.toFile(), lockExclusively);
            }
        });
    }

    /**
     * Open a reading stream from file at supplied path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - File at supplied path is not a regular one.
     *
     * @param path A file-system path.
     * @return Opened input.
     */
    public final Input openInput(Path path) {
        return openInput(path, false);
    }

    /**
     * Open a reading stream from file at supplied path. This transaction will be transparently committed at stream
     * closing. Transaction is also immediately detached from current thread, unless an error happens and prevents input
     * from being returned.
     *
     * @param path A file-system path.
     * @return Opened input.
     */
    public final Input openCommittingInput(Path path) {
        return openInput(path, true);
    }

    private Input openInput(final Path path, final boolean commitOnClose) {
        return inLock(new TransactionFunction<Input>() {
            @Override
            public Input apply() throws XAApplicationException, InterruptedException {
                Input input = new Input(TransactionContext.this,
                                        session.createXAFileInputStream(path.toFile(), lockExclusively),
                                        commitOnClose);
                if (commitOnClose) {
                    detach();
                }
                return input;
            }
        });
    }

    /**
     * Open a writing (appending) stream to file at supplied path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - File at supplied path is not a regular one.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     * @return Opened output.
     */
    public abstract Output openOutput(Path path);

    /**
     * Open a writing (appending) stream to file at supplied path. Optimized for large writes.
     *
     * @param path A file-system path.
     * @return Opened output.
     */
    public abstract Output openHeavyWriteOutput(Path path);

    /**
     * Moves file or directory from supplied source path to supplied destination path. Fails if :<br>
     * - Source path does not exists.<br>
     * - Destination path already exists.<br>
     * - A stream is currently open on source path in this transaction.<br>
     * - This context is read-only.
     *
     * @param src Source file-system path.
     * @param dest Destination file-system path.
     */
    public abstract void move(Path src, Path dest);

    /**
     * Create a regular file at supplied path. Fails if :<br>
     * - Supplied path already exists.<br>
     * - Parent path is not a directory.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     */
    public abstract void create(Path path);

    /**
     * Delete file or (empty) directory at specified path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - A stream is currently open on supplied path in this transaction.<br>
     * - File at supplied path is a non-empty directory.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     */
    public abstract void delete(Path path);

    /**
     * Truncate file at spectified path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - File at supplied path is not a regular one.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     * @param length Length in byte to truncate file to. Expected to be non-negative and less than or equal to current
     * file length.
     */
    public abstract void truncate(Path path, long length);
}
