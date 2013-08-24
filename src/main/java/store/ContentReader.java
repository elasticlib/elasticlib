package store;

import java.io.InputStream;
import store.info.ContentInfo;

public class ContentReader {

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
}
