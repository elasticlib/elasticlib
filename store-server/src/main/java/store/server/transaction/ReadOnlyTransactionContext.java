package store.server.transaction;

import java.io.File;
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
    public Input openInput(Path path) {
        try {
            File file = path.toFile();
            if (!session.fileExists(file)) {
                return EmptyInput.INSTANCE;
            }
            return new Input(session.createXAFileInputStream(file));

        } catch (LockingFailedException |
                NoTransactionAssociatedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Output openTruncatingOutput(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output openAppendingOutput(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output openHeavyWriteOutput(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Path path) {
        throw new UnsupportedOperationException();
    }
}
