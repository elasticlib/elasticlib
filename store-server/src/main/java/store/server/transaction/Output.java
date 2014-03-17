package store.server.transaction;

import java.io.OutputStream;
import org.xadisk.bridge.proxies.interfaces.XAFileOutputStream;
import org.xadisk.filesystem.exceptions.ClosedStreamException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import store.server.exception.RepositoryNotStartedException;

/**
 * A transactional output stream.
 * <p>
 * Never throws any IOException.
 */
public class Output extends OutputStream {

    private final TransactionContext txContext;
    private final XAFileOutputStream delegate;

    Output(TransactionContext txContext, XAFileOutputStream delegate) {
        this.txContext = txContext;
        this.delegate = delegate;
    }

    @Override
    public void write(int b) {
        try {
            delegate.write(b);

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void write(byte[] bytes) {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        try {
            delegate.write(bytes, offset, length);

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void flush() {
        try {
            delegate.flush();

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        try {
            delegate.close();

        } catch (NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new RepositoryNotStartedException(e);
            }
            throw new IllegalStateException(e);
        }
    }
}
