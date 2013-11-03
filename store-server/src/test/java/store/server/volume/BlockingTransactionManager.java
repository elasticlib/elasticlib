package store.server.volume;

import java.nio.file.Path;
import store.common.Hash;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionManager;

public class BlockingTransactionManager extends TransactionManager {

    private boolean ready;
    private boolean proceed;

    public BlockingTransactionManager(Path path) {
        super(path);
    }

    public synchronized void awaitReady() {
        while (!ready) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

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
