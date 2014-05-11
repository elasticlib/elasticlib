package store.server.storage;

import com.sleepycat.je.Transaction;
import java.util.concurrent.atomic.AtomicReference;

class SuspendedTransaction {

    private static enum State {

        SUSPENDED, RESUMED, ABORTED;
    }
    private final Transaction transaction;
    private final AtomicReference<State> state = new AtomicReference<>(State.SUSPENDED);

    public SuspendedTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public void abortIfSuspended() {
        if (state.compareAndSet(State.SUSPENDED, State.ABORTED)) {
            transaction.abort();
        }
    }

    public boolean resume() {
        return state.compareAndSet(State.SUSPENDED, State.RESUMED);
    }

    public Transaction getTransaction() {
        return transaction;
    }
}
