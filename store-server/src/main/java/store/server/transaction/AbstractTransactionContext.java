package store.server.transaction;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.filesystem.exceptions.FileNotExistsException;
import org.xadisk.filesystem.exceptions.InsufficientPermissionOnFileException;
import org.xadisk.filesystem.exceptions.LockingFailedException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import store.server.exception.RepositoryNotStartedException;

abstract class AbstractTransactionContext implements TransactionContext {

    final Session session;
    final AtomicBoolean closed = new AtomicBoolean(false);
    private final TransactionManager transactionManager;
    private final boolean lockExclusively;

    public AbstractTransactionContext(TransactionManager transactionManager, Session session, boolean lockExclusively) {
        this.transactionManager = transactionManager;
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
            transactionManager.remove(this);

        } catch (NoTransactionAssociatedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            session.rollback();
            transactionManager.remove(this);

        } catch (NoTransactionAssociatedException e) {
            throw new IllegalStateException(e);
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
                throw new RepositoryNotStartedException();
            }
            throw new IllegalStateException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String[] listFiles(Path path) {
        try {
            return session.listFiles(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException();
            }
            throw new RuntimeException(e);

        } catch (FileNotExistsException |
                LockingFailedException |
                InterruptedException |
                InsufficientPermissionOnFileException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final long fileLength(Path path) {
        try {
            return session.getFileLength(path.toFile(), lockExclusively);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException();
            }
            throw new IllegalStateException(e);

        } catch (FileNotExistsException |
                LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final Input openInput(Path path) {
        return openInput(path, false);
    }

    @Override
    public final Input openCommitingInput(Path path) {
        return openInput(path, true);
    }

    private Input openInput(Path path, boolean commitOnClose) {
        try {
            return new Input(this, session.createXAFileInputStream(path.toFile(), lockExclusively), commitOnClose);

        } catch (NoTransactionAssociatedException e) {
            if (closed.get()) {
                throw new RepositoryNotStartedException();
            }
            throw new IllegalStateException(e);

        } catch (LockingFailedException |
                InsufficientPermissionOnFileException |
                InterruptedException |
                FileNotExistsException e) {
            throw new IllegalStateException(e);
        }
    }
}
