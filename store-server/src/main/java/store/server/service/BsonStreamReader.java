package store.server.service;

import java.io.Closeable;
import static java.nio.ByteBuffer.wrap;
import java.util.NoSuchElementException;
import store.common.bson.BsonReader;
import store.server.transaction.Input;

class BsonStreamReader implements Closeable {

    private static final String UNEXPECTED_END_OF_STREAM = "Unexpected end of stream";
    private static final int EOF = -1;
    private final Input input;
    private BsonReader next;
    private long position = 0;
    private boolean loaded;

    public BsonStreamReader(Input input) {
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

    public BsonReader next() {
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

    private BsonReader loadNext() {
        int length = readInt();
        if (length == 0) {
            return null;
        }
        return new BsonReader(readBytes(length));
    }

    private int readInt() {
        byte[] bytes = new byte[4];
        if (input.read(bytes) == EOF) {
            return 0;
        }
        if (bytes.length != 4) {
            throw new IllegalStateException(UNEXPECTED_END_OF_STREAM);
        }
        return wrap(bytes)
                .getInt();
    }

    private byte[] readBytes(int length) {
        byte[] bytes = new byte[length];
        if (input.read(bytes) != length) {
            throw new IllegalStateException(UNEXPECTED_END_OF_STREAM);
        }
        return bytes;
    }
}
