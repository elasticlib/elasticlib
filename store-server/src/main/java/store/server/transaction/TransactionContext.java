package store.server.transaction;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileInputStream;
import org.xadisk.filesystem.exceptions.FileNotExistsException;
import org.xadisk.filesystem.exceptions.InsufficientPermissionOnFileException;
import org.xadisk.filesystem.exceptions.LockingFailedException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import store.server.exception.RepositoryNotStartedException;
import static store.server.transaction.TransactionManager.detachFromCurrentThread;

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

    final Session session;
    final AtomicBoolean closed = new AtomicBoolean(false);
    private final TransactionManager transactionManager;
    private final boolean lockExclusively;

    TransactionContext(TransactionManager transactionManager, Session session, boolean lockExclusively) {
        this.transactionManager = transactionManager;
        this.session = session;
        this.lockExclusively = lockExclusively;
    }

    /**
     * Persists all changes done in this transaction and close this context. Does nothing if this context is already
     * closed.
     */
    public final void commit() {
        close(true, true);
        detachFromCurrentThread();
    }

    /**
     * Abort all changes done in this transaction and close this context. Does nothing if this context is already
     * closed.
     */
    public final void close() {
        close(false, true);
        detachFromCurrentThread();
    }

    void close(boolean commit, boolean detachFromCurrentThread) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (commit) {
                session.commit();
            } else {
                session.rollback();
            }
            transactionManager.remove(this);

        } catch (NoTransactionAssociatedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Checks if this context is closed. If this is the case, other operations will fail (excepted closing ones, which
     * are idempotent).
     *
     * @return true if this context is closed.
     */
    public final boolean isClosed() {
        return closed.get();
    }

    /**
     * Checks if a file exists at supplied path.
     *
     * @param path A file-system path.
     * @return true if this is the case.
     */
    public final boolean exists(Path path) {
        try {
            return session.fileExists(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Lists file names at supplied path. Fails if file at supplied path does not exists or is not a directory.
     *
     * @param path A file-system path.
     * @return An array of file names.
     */
    public final String[] listFiles(Path path) {
        try {
            return session.listFiles(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new RuntimeException(e);

        } catch (FileNotExistsException |
                LockingFailedException |
                InterruptedException |
                InsufficientPermissionOnFileException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Provides length of file at supplied path. Fails if file at supplied path does not exists or is not a regular one.
     *
     * @param path A file-system path.
     * @return A length in bytes.
     */
    public final long fileLength(Path path) {
        try {
            return session.getFileLength(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (FileNotExistsException |
                LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
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
    public final Input openCommitingInput(Path path) {
        return openInput(path, true);
    }

    private Input openInput(Path path, boolean commitOnClose) {
        XAFileInputStream inputStream;
        try {
            inputStream = session.createXAFileInputStream(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException e) {
            throw new IllegalStateException(e);
        }

        if (commitOnClose) {
            detachFromCurrentThread();
        }
        return new Input(this, inputStream, commitOnClose);
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
