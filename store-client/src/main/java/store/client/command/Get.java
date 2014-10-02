package store.client.command;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import static java.nio.file.Files.newOutputStream;
import java.nio.file.Path;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.client.util.Directories;
import store.common.client.Content;
import store.common.hash.Hash;
import static store.common.util.IoUtil.copy;

class Get extends AbstractCommand {

    Get() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Get an existing content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        try {
            try (Content content = session.getRepository().getContent(hash)) {
                Path path = Directories.resolve(content.getFileName().or(hash.asHexadecimalString()));
                try (InputStream inputStream = content.getInputStream();
                        OutputStream outputStream = new DefferedFileOutputStream(path)) {
                    copy(inputStream, outputStream);
                }
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    /**
     * A file output stream that create or truncate file in a lazy way, only when first byte is written.
     */
    private static class DefferedFileOutputStream extends OutputStream {

        private final Path path;
        private Optional<OutputStream> outputStream;

        /**
         * Constructor.
         *
         * @param path Path to write to.
         */
        public DefferedFileOutputStream(Path path) {
            this.path = path;
            outputStream = Optional.absent();
        }

        @Override
        public void write(int b) throws IOException {
            if (!outputStream.isPresent()) {
                outputStream = Optional.of(newOutputStream(path));
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
                outputStream = Optional.of(newOutputStream(path));
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
}
