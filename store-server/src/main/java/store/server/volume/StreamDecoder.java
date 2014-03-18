package store.server.volume;

import java.io.Closeable;
import static java.nio.ByteBuffer.wrap;
import java.util.NoSuchElementException;
import store.common.io.ObjectDecoder;
import store.server.transaction.Input;

class StreamDecoder implements Closeable {

    private static final int EOF = -1;
    private final Input input;
    private ObjectDecoder next;
    private long position = 0;
    private boolean loaded;

    public StreamDecoder(Input input) {
        this.input = input;
    }

    public boolean hasNext() {
        if (!loaded) {
            position = input.position();
            next = loadNext();
            loaded = true;
        }
        return next != null;
    }

    public ObjectDecoder next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        loaded = false;
        return next;
    }

    public long position() {
        return position;
    }

    @Override
    public void close() {
        input.close();
    }

    private ObjectDecoder loadNext() {
        int length = readInt();
        if (length == 0) {
            return null;
        }
        return new ObjectDecoder(readBytes(length));
    }

    private int readInt() {
        byte[] bytes = new byte[4];
        if (input.read(bytes) == EOF) {
            return 0;
        }
        if (bytes.length != 4) {
            throw new IllegalStateException("Unexpected end of stream");
        }
        return wrap(bytes)
                .getInt();
    }

    private byte[] readBytes(int length) {
        byte[] bytes = new byte[length];
        if (input.read(bytes) != length) {
            throw new IllegalStateException("Unexpected end of stream");
        }
        return bytes;
    }
}
