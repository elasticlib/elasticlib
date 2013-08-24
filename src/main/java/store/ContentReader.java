package store;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import store.info.ContentInfo;

public class ContentReader implements Closeable {

    private final ContentInfo info;
    private final InputStream inputStream;

    public ContentReader(ContentInfo info, InputStream inputStream) {
        this.info = info;
        this.inputStream = inputStream;
    }

    public ContentInfo info() {
        return info;
    }

    public InputStream inputStream() {
        return inputStream;
    }

    public void close() {
        try {
            inputStream.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
