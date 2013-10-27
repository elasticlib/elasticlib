package store.server.transaction;

import java.io.InputStream;
import org.xadisk.bridge.proxies.interfaces.XAFileInputStream;
import org.xadisk.filesystem.exceptions.ClosedStreamException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;
import store.server.exception.VolumeClosedException;

public class Input extends InputStream {

    private final TransactionContext txContext;
    private final XAFileInputStream delegate;

    public Input(TransactionContext txContext, XAFileInputStream delegate) {
        this.txContext = txContext;
        this.delegate = delegate;
    }

    @Override
    public int read() {
        try {
            return delegate.read();

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new VolumeClosedException();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public int read(byte[] bytes) {
        return read(bytes, 0, bytes.length);
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        try {
            int pos = delegate.read(bytes, offset, length);
            if (pos == -1) {
                return pos;
            }
            while (pos != length) {
                if (pos > length) {
                    throw new RuntimeException();
                }
                int ret = delegate.read(bytes, offset + pos, length - pos);
                if (ret == -1) {
                    return pos;
                }
                pos += ret;
            }
            return pos;

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new VolumeClosedException();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public long skip(long n) {
        try {
            return delegate.skip(n);

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new VolumeClosedException();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public int available() {
        try {
            return delegate.available();

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new VolumeClosedException();
            }
            throw new RuntimeException(e);
        }
    }

    public long position() {
        return delegate.position();
    }

    public void position(long n) {
        try {
            delegate.position(n);

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new VolumeClosedException();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            delegate.close();

        } catch (NoTransactionAssociatedException e) {
            if (txContext.isClosed()) {
                throw new VolumeClosedException();
            }
            throw new RuntimeException(e);
        }
    }
}
