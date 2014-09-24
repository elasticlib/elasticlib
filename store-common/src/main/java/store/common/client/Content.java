package store.common.client;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;

/**
 * Represents a handle on a content in a repository.
 */
public class Content implements AutoCloseable {

    private final Optional<String> fileName;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param fileName Optional content file name.
     * @param inputStream Content input-stream.
     */
    public Content(Optional<String> fileName, InputStream inputStream) {
        this.fileName = fileName;
        this.inputStream = inputStream;
    }

    /**
     * @return Content file name, if any.
     */
    public Optional<String> getFileName() {
        return fileName;
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
