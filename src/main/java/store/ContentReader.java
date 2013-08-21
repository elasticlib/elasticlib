package store;

import store.info.ContentInfo;
import java.io.InputStream;

public class ContentReader {

    private final ContentInfo info;

    public ContentReader(ContentInfo info) {
        this.info = info;
    }

    public ContentInfo info() {
        return info;
    }

    public InputStream inputStream() {
        return null; // TODO
    }
}
