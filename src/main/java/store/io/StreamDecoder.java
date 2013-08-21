package store.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import static java.nio.ByteBuffer.wrap;
import java.util.NoSuchElementException;

public class StreamDecoder implements Closeable {

    private static final int EOF = -1;
    private final InputStream inputStream;
    private ObjectDecoder next;
    private boolean loaded;

    public StreamDecoder(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public boolean hasNext() throws IOException {
        if (!loaded) {
            next = loadNext();
            loaded = true;
        }
        return next != null;
    }

    public ObjectDecoder next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        loaded = false;
        return next;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    private ObjectDecoder loadNext() throws IOException {
        int length = readInt();
        if (length == 0) {
            return null;
        }
        return new ObjectDecoder(readBytes(length));
    }

    private int readInt() throws IOException {
        byte[] bytes = new byte[4];
        if (inputStream.read(bytes) == EOF) {
            return 0;
        }
        if (bytes.length != 4) {
            throw new IOException("Unexpected end of stream");
        }
        return wrap(bytes)
                .getInt();
    }

    private byte[] readBytes(int length) throws IOException {
        byte[] bytes = new byte[length];
        if (inputStream.read(bytes) != length) {
            throw new IOException("Unexpected end of stream");
        }
        return bytes;
    }
}
