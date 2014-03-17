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
import store.server.exception.RepositoryNotStartedException;

final class ReadWriteTransactionContext extends TransactionContext {

    ReadWriteTransactionContext(TransactionManager transactionManager, Session session) {
        super(transactionManager, session, true);
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
            return new Output(this, session.createXAFileOutputStream(file, heavyWrite));

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException |
                FileUnderUseException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void move(Path src, Path dest) {
        try {
            session.moveFile(src.toFile(), dest.toFile());

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (FileAlreadyExistsException |
                FileNotExistsException |
                FileUnderUseException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void create(Path path) {
        try {
            session.createFile(path.toFile(), false);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (FileAlreadyExistsException |
                FileNotExistsException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void delete(Path path) {
        try {
            session.deleteFile(path.toFile());

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (DirectoryNotEmptyException |
                FileNotExistsException |
                FileUnderUseException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void truncate(Path path, long length) {
        try {
            session.truncateFile(path.toFile(), length);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);

        } catch (FileNotExistsException |
                InsufficientPermissionOnFileException |
                LockingFailedException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
