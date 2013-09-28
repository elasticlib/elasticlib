package store.server.transaction;

import java.io.InputStream;
import org.xadisk.bridge.proxies.interfaces.XAFileInputStream;
import org.xadisk.filesystem.exceptions.ClosedStreamException;
import org.xadisk.filesystem.exceptions.NoTransactionAssociatedException;

public class Input extends InputStream {

    private final XAFileInputStream delegate;

    public Input(XAFileInputStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public int read() {
        try {
            return delegate.read();

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
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
            return delegate.read(bytes, offset, length);

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long skip(long n) {
        try {
            return delegate.skip(n);

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int available() {
        try {
            return delegate.available();

        } catch (ClosedStreamException | NoTransactionAssociatedException e) {
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
            throw new RuntimeException(e);
        }
    }
}
