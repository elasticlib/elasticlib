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
    public boolean exists(Path path) {
        try {
            return session.fileExists(path.toFile(), true);

        } catch (LockingFailedException |
                NoTransactionAssociatedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void create(Path path) {
        try {
            session.createFile(path.toFile(), false);

        } catch (FileAlreadyExistsException |
                FileNotExistsException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                NoTransactionAssociatedException |
                InterruptedException e) {
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

    @Override
    public void truncate(Path path, long length) {
        try {
            session.truncateFile(path.toFile(), length);

        } catch (FileNotExistsException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                NoTransactionAssociatedException |
                InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Input openInput(Path path) {
        try {
            return new Input(session.createXAFileInputStream(path.toFile(), true));

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
        return openOutput(path, false);
    }

    @Override
    public Output openHeavyWriteOutput(Path path) {
        return openOutput(path, true);
    }

    private Output openOutput(Path path, boolean heavyWrite) {
        try {
            File file = path.toFile();
            return new Output(session.createXAFileOutputStream(file, heavyWrite));

        } catch (LockingFailedException |
                NoTransactionAssociatedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException |
                FileUnderUseException e) {
            throw new RuntimeException(e);
        }
    }
}
