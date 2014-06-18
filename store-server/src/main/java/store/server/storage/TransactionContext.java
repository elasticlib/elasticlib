package store.server.storage;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Transaction;
import java.util.Deque;
import static java.util.Objects.hash;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Holds a transaction with its state and cursors.
 */
class TransactionContext {

    private static enum State {

        RUNNING, SUSPENDED, CLOSED;
    }
    private final Transaction transaction;
    private final Deque<Cursor> cursors = new ConcurrentLinkedDeque<>();
    private State state = State.RUNNING;

    public TransactionContext(Transaction transaction) {
        this.transaction = transaction;
    }

    public long getId() {
        return transaction.getId();
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public synchronized void add(Cursor cursor) {
        if (state != State.RUNNING) {
            throw new IllegalStateException();
        }
        cursors.add(cursor);
    }

    public synchronized void abortIfSuspended() {
        if (state == State.SUSPENDED) {
            close(false);
        }
    }

    public synchronized void abortIfRunning() {
        if (state == State.RUNNING) {
            close(false);
        }
    }

    public synchronized void commitIfRunning() {
        if (state == State.RUNNING) {
            close(true);
        }
    }

    public synchronized void abort() {
        if (state != State.CLOSED) {
            close(false);
        }
    }

    private void close(boolean commit) {
        state = State.CLOSED;
        while (!cursors.isEmpty()) {
            cursors.remove().close();
        }
        if (commit) {
            transaction.commit();
        } else {
            transaction.abort();
        }
    }

    public synchronized void suspend() {
        if (!compareAndSet(State.RUNNING, State.SUSPENDED)) {
            throw new IllegalStateException();
        }
    }

    public synchronized boolean resume() {
        return compareAndSet(State.SUSPENDED, State.RUNNING);
    }

    private boolean compareAndSet(State expect, State update) {
        if (state != expect) {
            return false;
        }
        state = update;
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TransactionContext) {
            TransactionContext that = (TransactionContext) obj;
            return getId() == that.getId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash(getId());
    }
}
