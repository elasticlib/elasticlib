package store.server.transaction;

import java.io.OutputStream;
import org.xadisk.bridge.proxies.interfaces.XAFileOutputStream;
import org.xadisk.filesystem.exceptions.XAApplicationException;
import store.server.transaction.TransactionContext.TransactionProcedure;

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
    public void write(final int b) {
        txContext.inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException {
                delegate.write(b);
            }
        });
    }

    @Override
    public void write(byte[] bytes) {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
        txContext.inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException {
                delegate.write(bytes, offset, length);
            }
        });
    }

    @Override
    public void flush() {
        txContext.inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException {
                delegate.flush();
            }
        });
    }

    @Override
    public void close() {
        txContext.inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException {
                delegate.close();
            }
        });
    }
}
