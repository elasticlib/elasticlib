package store.client.http;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A file output stream that create or truncate file in a lazy way, only when first byte is written.
 */
class DefferedFileOutputStream extends OutputStream {

    private final Path path;
    private Optional<OutputStream> outputStream;

    public DefferedFileOutputStream(Path path) {
        this.path = path;
        outputStream = Optional.absent();
    }

    @Override
    public void write(int b) throws IOException {
        if (!outputStream.isPresent()) {
            outputStream = Optional.of(Files.newOutputStream(path));
        }
        outputStream.get().write(b);
    }

    @Override
    public void write(byte[] buf) throws IOException {
        write(buf, 0, buf.length);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        if (len == 0) {
            return;
        }
        if (!outputStream.isPresent()) {
            outputStream = Optional.of(Files.newOutputStream(path));
        }
        outputStream.get().write(buf, off, len);
    }

    @Override
    public void flush() throws IOException {
        if (outputStream.isPresent()) {
            outputStream.get().flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (outputStream.isPresent()) {
            outputStream.get().close();
        }
    }
}
