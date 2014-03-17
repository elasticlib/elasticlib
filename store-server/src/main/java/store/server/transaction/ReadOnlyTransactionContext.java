package store.server.transaction;

import java.nio.file.Path;
import org.xadisk.bridge.proxies.interfaces.Session;

final class ReadOnlyTransactionContext extends TransactionContext {

    ReadOnlyTransactionContext(TransactionManager transactionManager, Session session) {
        super(transactionManager, session, false);
    }

    @Override
    public Output openOutput(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output openHeavyWriteOutput(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void move(Path src, Path dest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void create(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncate(Path path, long length) {
        throw new UnsupportedOperationException();
    }
}
