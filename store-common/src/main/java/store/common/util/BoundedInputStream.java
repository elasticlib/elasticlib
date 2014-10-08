package store.common.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input-stream that read at most a specified amount of bytes. Its close() method does nothing, caller is responsible
 * for closing underlying input-stream himself.
 */
public class BoundedInputStream extends InputStream {

    private final InputStream delegate;
    private long remaining;

    /**
     * Constructor.
     *
     * @param delegate Delegate stream to read from.
     * @param limit Maximum amount of bytes to read.
     */
    public BoundedInputStream(InputStream delegate, long limit) {
        this.delegate = delegate;
        remaining = limit;
    }

    @Override
    public int read() throws IOException {
        if (remaining <= 0) {
            return -1;
        }
        --remaining;
        return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (remaining <= 0) {
            return -1;
        }
        int maxLen = len <= remaining ? len : (int) remaining;
        int readLen = delegate.read(b, off, maxLen);
        if (readLen >= 0) {
            remaining -= readLen;
        }
        return readLen;
    }

    @Override
    public void close() {
        // Does nothing.
    }
}
