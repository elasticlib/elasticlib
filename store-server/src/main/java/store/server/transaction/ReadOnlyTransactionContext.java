package store.server.transaction;

import java.nio.file.Path;
import org.xadisk.bridge.proxies.interfaces.Session;

final class ReadOnlyTransactionContext extends AbstractTransactionContext {

    public ReadOnlyTransactionContext(Session session) {
        super(session, false);
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

    @Override
    public Output openOutput(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output openHeavyWriteOutput(Path path) {
        throw new UnsupportedOperationException();
    }
}
