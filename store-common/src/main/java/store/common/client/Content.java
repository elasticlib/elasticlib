package store.common.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import javax.ws.rs.core.MediaType;

/**
 * Represents a handle on a content in a repository.
 */
public class Content implements AutoCloseable {

    private final Optional<String> fileName;
    private final MediaType mediaType;
    private final long length;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param fileName Optional content file name.
     * @param mediaType Content media type.
     * @param length Content length in bytes.
     * @param inputStream Content input-stream.
     */
    public Content(Optional<String> fileName, MediaType mediaType, long length, InputStream inputStream) {
        this.fileName = fileName;
        this.mediaType = mediaType;
        this.length = length;
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
     * @return Content length in bytes.
     */
    public long getLength() {
        return length;
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
