package store.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import store.server.exception.StoreRuntimeException;
import store.common.ContentInfo;

public class ContentReader implements Closeable {

    private final Store store;
    private final ContentInfo info;
    private final InputStream inputStream;
    private boolean isOpen = true;

    public ContentReader(Store store, ContentInfo info, InputStream inputStream) {
        this.store = store;
        this.info = info;
        this.inputStream = inputStream;
    }

    public ContentInfo info() {
        return info;
    }

    public InputStream inputStream() {
        if (!isOpen) {
            throw new IllegalStateException();
        }
        return inputStream;
    }

    @Override
    public void close() {
        if (!isOpen) {
            return;
        }
        try {
            inputStream.close();
            isOpen = false;
            store.close(info.getHash());

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }
}
