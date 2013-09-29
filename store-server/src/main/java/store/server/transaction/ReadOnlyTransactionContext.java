package store.server.transaction;

import java.nio.file.Path;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.filesystem.exceptions.FileNotExistsException;
import org.xadisk.filesystem.exceptions.InsufficientPermissionOnFileException;
import org.xadisk.filesystem.exceptions.LockingFailedException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;

final class ReadOnlyTransactionContext implements TransactionContext {

    private final Session session;

    public ReadOnlyTransactionContext(Session session) {
        this.session = session;
    }

    @Override
    public boolean exists(Path path) {
        try {
            return session.fileExists(path.toFile(), false);

        } catch (LockingFailedException |
                NoTransactionAssociatedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new RuntimeException(e);
        }
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
    public Input openInput(Path path) {
        try {
            return new Input(session.createXAFileInputStream(path.toFile()));

        } catch (LockingFailedException |
                NoTransactionAssociatedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException e) {
            throw new RuntimeException(e);
        }
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
