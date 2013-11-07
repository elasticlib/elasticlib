package store.server.volume;

import java.nio.file.Path;
import store.common.Hash;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionManager;

/**
 * A wrapping transaction manager providing a hook to block execution during a transaction for testing purpose.
 *
 * @author Guillaume Masclet (guillaume.masclet@gadz.org)
 */
class BlockingTransactionManager extends TransactionManager {

    private boolean ready;
    private boolean proceed;

    public BlockingTransactionManager(Path path) {
        super(path);
    }

    /**
     * Waits until at least a thread has started a transaction and is ready to proceed.
     */
    public synchronized void awaitReady() {
        while (!ready) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Unblocks waiting threads, if any.
     */
    public synchronized void proceed() {
        proceed = true;
        notifyAll();
    }

    @Override
    public <T> T inTransaction(final Query<T> query) {
        // On ne surcharge pas l'autre variante de inTransaction(Hash, Query). Elle appelle déjà implicitement celle-ci.
        return super.inTransaction(new Query<T>() {
            @Override
            public T apply() {
                awaitProceed();
                return query.apply();
            }
        });
    }

    @Override
    public void inTransaction(Hash hash, final Command command) {
        super.inTransaction(hash, new Command() {
            @Override
            public void apply() {
                awaitProceed();
                command.apply();
            }
        });
    }

    private synchronized void awaitProceed() {
        ready = true;
        notifyAll();
        while (!proceed) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}