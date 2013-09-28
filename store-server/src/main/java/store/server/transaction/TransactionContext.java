package store.server.transaction;

import java.nio.file.Path;

public interface TransactionContext {

    Input openInput(Path path);

    Output openTruncatingOutput(Path path);

    Output openAppendingOutput(Path path);

    Output openHeavyWriteOutput(Path path);

    void delete(Path path);
}
