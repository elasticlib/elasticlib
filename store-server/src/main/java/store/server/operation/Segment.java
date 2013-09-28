package store.server.operation;

import java.nio.file.Path;
import java.util.Date;
import store.common.Hash;
import store.server.Uid;
import static store.server.io.ObjectEncoder.encoder;
import store.server.transaction.Output;
import static store.server.transaction.TransactionManager.currentTransactionContext;

final class Segment {

    private final Path path;

    public Segment(Path path) {
        this.path = path;
    }

    public synchronized void add(Uid uid, Hash hash, OpCode opCode) {
        Step step = new Step(uid, hash, new Date().getTime(), opCode);
        byte[] bytes = encoder()
                .put("uid", step.getUid().value())
                .put("hash", step.getHash().value())
                .put("timestamp", step.getTimestamp())
                .put("OpCode", step.getOpCode().value())
                .build();

        try (Output output = currentTransactionContext().openAppendingOutput(path)) {
            output.write(bytes);
        }
    }
}
