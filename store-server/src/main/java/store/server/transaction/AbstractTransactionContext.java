package store.server.transaction;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.filesystem.exceptions.FileNotExistsException;
import org.xadisk.filesystem.exceptions.InsufficientPermissionOnFileException;
import org.xadisk.filesystem.exceptions.LockingFailedException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import store.server.exception.VolumeNotStartedException;

public abstract class AbstractTransactionContext implements TransactionContext {

    final Session session;
    final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean lockExclusively;

    public AbstractTransactionContext(Session session, boolean lockExclusively) {
        this.session = session;
        this.lockExclusively = lockExclusively;
    }

    @Override
    public final void commit() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            session.commit();

        } catch (NoTransactionAssociatedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            session.rollback();

        } catch (NoTransactionAssociatedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public final boolean exists(Path path) {
        try {
            return session.fileExists(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new VolumeNotStartedException();
            }
            throw new RuntimeException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final String[] listFiles(Path path) {
        try {
            return session.listFiles(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new VolumeNotStartedException();
            }
            throw new RuntimeException(e);

        } catch (FileNotExistsException |
                LockingFailedException |
                InterruptedException |
                InsufficientPermissionOnFileException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final long fileLength(Path path) {
        try {
            return session.getFileLength(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new VolumeNotStartedException();
            }
            throw new RuntimeException(e);

        } catch (FileNotExistsException |
                LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final Input openInput(Path path) {
        try {
            return new Input(this, session.createXAFileInputStream(path.toFile(), lockExclusively));

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new VolumeNotStartedException();
            }
            throw new RuntimeException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException e) {
            throw new RuntimeException(e);
        }
    }
}
