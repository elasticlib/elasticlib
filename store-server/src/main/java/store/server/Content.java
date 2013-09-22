package store.server;

import java.io.InputStream;
import store.common.ContentInfo;

public class Content {

    private final ContentInfo info;
    private final InputStream inputStream;

    public Content(ContentInfo info, InputStream inputStream) {
        this.info = info;
        this.inputStream = inputStream;
    }

    public ContentReader reader(Store store) {
        return new ContentReader(store, info, inputStream);
    }
}
