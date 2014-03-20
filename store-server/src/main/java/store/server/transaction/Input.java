package store.server.transaction;

import java.io.InputStream;
import org.xadisk.bridge.proxies.interfaces.XAFileInputStream;
import org.xadisk.filesystem.exceptions.XAApplicationException;
import store.server.transaction.TransactionContext.TransactionFunction;
import store.server.transaction.TransactionContext.TransactionProcedure;

/**
 * A transactional input stream.
 * <p>
 * Does not support mark/reset operations. Never throws any IOException.
 */
public class Input extends InputStream {

    private final TransactionContext txContext;
    private final XAFileInputStream delegate;
    private final boolean commitOnClose;
    private boolean closed = false;

    Input(TransactionContext txContext, XAFileInputStream delegate, boolean commitOnClose) {
        this.txContext = txContext;
        this.delegate = delegate;
        this.commitOnClose = commitOnClose;
    }

    @Override
    public int read() {
        return txContext.inLock(new TransactionFunction<Integer>() {
            @Override
            public Integer apply() throws XAApplicationException {
                return delegate.read();
            }
        });
    }

    @Override
    public int read(byte[] bytes) {
        return read(bytes, 0, bytes.length);
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) {
        return txContext.inLock(new TransactionFunction<Integer>() {
            @Override
            public Integer apply() throws XAApplicationException {
                int pos = delegate.read(bytes, offset, length);
                if (pos == -1) {
                    return pos;
                }
                while (pos < length) {
                    int ret = delegate.read(bytes, offset + pos, length - pos);
                    if (ret == -1) {
                        return pos;
                    }
                    pos += ret;
                }
                return pos;
            }
        });
    }

    @Override
    public long skip(final long n) {
        return txContext.inLock(new TransactionFunction<Long>() {
            @Override
            public Long apply() throws XAApplicationException {
                return delegate.skip(n);
            }
        });
    }

    @Override
    public int available() {
        return txContext.inLock(new TransactionFunction<Integer>() {
            @Override
            public Integer apply() throws XAApplicationException {
                return delegate.available();
            }
        });
    }

    /**
     * Provides zero-based position of reading pointer in the file.
     *
     * @return Absolute position in bytes in the file.
     */
    public long position() {
        return txContext.inLock(new TransactionFunction<Long>() {
            @Override
            public Long apply() {
                return delegate.position();
            }
        });
    }

    /**
     * Set position of reading pointer in the file.
     *
     * @param n New absolute position in bytes in the file. Expected to be positive and less or equal to file length.
     */
    public void position(final long n) {
        txContext.inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException {
                delegate.position(n);
            }
        });
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
        if (closed) {
            return;
        }
        txContext.inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException {
                delegate.close();
                if (commitOnClose) {
                    txContext.commit();
                }
            }
        });
        closed = true;
    }
}
