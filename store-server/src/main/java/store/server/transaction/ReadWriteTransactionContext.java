package store.server.transaction;

import java.nio.file.Path;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.filesystem.exceptions.XAApplicationException;

final class ReadWriteTransactionContext extends TransactionContext {

    private final Session session;

    ReadWriteTransactionContext(TransactionManager transactionManager, Session session) {
        super(transactionManager, session, true);
        this.session = session;
    }

    @Override
    public Output openOutput(Path path) {
        return openOutput(path, false);
    }

    @Override
    public Output openHeavyWriteOutput(Path path) {
        return openOutput(path, true);
    }

    private Output openOutput(final Path path, final boolean heavyWrite) {
        return inLock(new TransactionFunction<Output>() {
            @Override
            public Output apply() throws XAApplicationException, InterruptedException {
                return new Output(ReadWriteTransactionContext.this,
                                  session.createXAFileOutputStream(path.toFile(), heavyWrite));
            }
        });
    }

    @Override
    public void move(final Path src, final Path dest) {
        inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException, InterruptedException {
                session.moveFile(src.toFile(), dest.toFile());
            }
        });
    }

    @Override
    public void create(final Path path) {
        inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException, InterruptedException {
                session.createFile(path.toFile(), false);
            }
        });
    }

    @Override
    public void delete(final Path path) {
        inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException, InterruptedException {
                session.deleteFile(path.toFile());
            }
        });
    }

    @Override
    public void truncate(final Path path, final long length) {
        inLock(new TransactionProcedure() {
            @Override
            public void apply() throws XAApplicationException, InterruptedException {
                session.truncateFile(path.toFile(), length);
            }
        });
    }
}
