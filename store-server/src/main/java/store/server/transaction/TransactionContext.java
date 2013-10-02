package store.server.transaction;

import java.nio.file.Path;

public interface TransactionContext {

    boolean exists(Path path);

    String[] listFiles(Path path);

    long fileLength(Path path);

    void create(Path path);

    void delete(Path path);

    void truncate(Path path, long length);

    Input openInput(Path path);

    Output openOutput(Path path);

    Output openHeavyWriteOutput(Path path);
}
