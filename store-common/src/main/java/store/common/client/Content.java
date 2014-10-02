package store.common.client;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.MediaType;

/**
 * Represents a handle on a content in a repository.
 */
public class Content implements AutoCloseable {

    private final Optional<String> fileName;
    private final MediaType mediaType;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param fileName Optional content file name.
     * @param mediaType Content media type.
     * @param inputStream Content input-stream.
     */
    public Content(Optional<String> fileName, MediaType mediaType, InputStream inputStream) {
        this.fileName = fileName;
        this.mediaType = mediaType;
        this.inputStream = inputStream;
    }

    /**
     * @return Content file name, if any.
     */
    public Optional<String> getFileName() {
        return fileName;
    }

    /**
     * @return Content type.
     */
    public MediaType getContentType() {
        return mediaType;
    }

    /**
     * @return Content input-stream.
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
