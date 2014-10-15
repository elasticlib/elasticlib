package store.server.manager.storage;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Transaction;
import java.util.Deque;
import static java.util.Objects.hash;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Holds a transaction with its state and cursors.
 */
class TransactionContext {

    private final Transaction transaction;
    private final Deque<Cursor> cursors = new ConcurrentLinkedDeque<>();
    private boolean closed;

    /**
     * Constructor.
     *
     * @param transaction Underlying JE transaction.
     */
    public TransactionContext(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * @return The underlying JE transaction.
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * Associates a cursor to this context.
     *
     * @param cursor A JE cursor.
     */
    public synchronized void add(Cursor cursor) {
        if (closed) {
            throw new IllegalStateException();
        }
        cursors.add(cursor);
    }

    /**
     * Commit underlying transaction and close all previously associated cursors, unless this context is already closed.
     */
    public synchronized void commit() {
        if (close()) {
            transaction.commit();
        }
    }

    /**
     * Abort underlying transaction and close all previously associated cursors, unless this context is already closed.
     */
    public synchronized void abort() {
        if (close()) {
            transaction.abort();
        }
    }

    private boolean close() {
        if (closed) {
            return false;
        }
        closed = true;
        while (!cursors.isEmpty()) {
            cursors.remove().close();
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TransactionContext) {
            TransactionContext that = (TransactionContext) obj;
            return transaction.getId() == that.transaction.getId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash(transaction.getId());
    }
}
