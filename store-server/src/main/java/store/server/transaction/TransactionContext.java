package store.server.transaction;

import java.nio.file.Path;

/**
 * A transaction context.
 * <p>
 * Provides access to file-system in a transactional fashion : all operations in a context executed atomically and are
 * isoled from non-commited operations in concurrent contexts. Once transaction is commited, all modifications are
 * durably written on disk.
 * <p>
 * A context may be read-write or read-only :<br>
 * - Read-write contexts operations are done with exclusive locking, whereas read-only contexts use shared locking.<br>
 * - All mutative operations require a read-write context.
 */
public interface TransactionContext {

    /**
     * Persists all changes done in this transaction and close this context. Does nothing if this context is already
     * closed.
     */
    void commit();

    /**
     * Abort all changes done in this transaction and close this context. Does nothing if this context is already
     * closed.
     */
    void close();

    /**
     * Checks if this context is closed. If this is the case, other operations will fail (excepted closing ones, which
     * are idempotent).
     *
     * @return true if this context is closed.
     */
    boolean isClosed();

    /**
     * Checks if a file exists at supplied path.
     *
     * @param path A file-system path.
     * @return true if this is the case.
     */
    boolean exists(Path path);

    /**
     * Lists file names at supplied path. Fails if file at supplied path does not exists or is not a directory.
     *
     * @param path A file-system path.
     * @return An array of file names.
     */
    String[] listFiles(Path path);

    /**
     * Provides length of file at supplied path. Fails if file at supplied path does not exists or is not a regular one.
     *
     * @param path A file-system path.
     * @return A length in bytes.
     */
    long fileLength(Path path);

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
    void move(Path src, Path dest);

    /**
     * Create a regular file at supplied path. Fails if :<br>
     * - Supplied path already exists.<br>
     * - Parent path is not a directory.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     */
    void create(Path path);

    /**
     * Delete file or (empty) directory at specified path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - A stream is currently open on supplied path in this transaction.<br>
     * - File at supplied path is a non-empty directory.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     */
    void delete(Path path);

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
    void truncate(Path path, long length);

    /**
     * Open a reading stream from file at supplied path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - File at supplied path is not a regular one.
     *
     * @param path A file-system path.
     * @return Opened input.
     */
    Input openInput(Path path);

    /**
     * Open a reading stream from file at supplied path. This context will be transparently committed at stream closing.
     *
     * @param path A file-system path.
     * @return Opened input.
     */
    Input openCommitingInput(Path path);

    /**
     * Open a writing (appending) stream to file at supplied path. Fails if :<br>
     * - Supplied path does not exists.<br>
     * - File at supplied path is not a regular one.<br>
     * - This context is read-only.
     *
     * @param path A file-system path.
     * @return Opened output.
     */
    Output openOutput(Path path);

    /**
     * Open a writing (appending) stream to file at supplied path. Optimized for large writes.
     *
     * @param path A file-system path.
     * @return Opened output.
     */
    Output openHeavyWriteOutput(Path path);
}
