package store.server.transaction;

import java.io.File;
import java.nio.file.Path;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.filesystem.exceptions.DirectoryNotEmptyException;
import org.xadisk.filesystem.exceptions.FileAlreadyExistsException;
import org.xadisk.filesystem.exceptions.FileNotExistsException;
import org.xadisk.filesystem.exceptions.FileUnderUseException;
import org.xadisk.filesystem.exceptions.InsufficientPermissionOnFileException;
import org.xadisk.filesystem.exceptions.LockingFailedException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;

final class ReadWriteTransactionContext implements TransactionContext {

    private final Session session;

    public ReadWriteTransactionContext(Session session) {
        this.session = session;
    }

    @Override
    public Input openInput(Path path) {
        try {
            File file = path.toFile();
            if (!session.fileExists(file, true)) {
                return EmptyInput.INSTANCE;
            }
            return new Input(session.createXAFileInputStream(file, true));

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
        return openOutput(path, true, false);
    }

    @Override
    public Output openAppendingOutput(Path path) {
        return openOutput(path, false, false);
    }

    @Override
    public Output openHeavyWriteOutput(Path path) {
        return openOutput(path, true, true);
    }

    private Output openOutput(Path path, boolean truncate, boolean heavyWrite) {
        try {
            File file = path.toFile();
            if (!session.fileExists(file, true)) {
                session.createFile(file, false);

            } else if (truncate) {
                session.truncateFile(file, 0);
            }
            return new Output(session.createXAFileOutputStream(file, heavyWrite));

        } catch (LockingFailedException |
                NoTransactionAssociatedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException |
                FileAlreadyExistsException |
                FileUnderUseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Path path) {
        try {
            session.deleteFile(path.toFile());

        } catch (DirectoryNotEmptyException |
                FileNotExistsException |
                FileUnderUseException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                NoTransactionAssociatedException |
                InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
