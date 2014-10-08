package store.common.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * An output-stream that writes to a random access file. Its close() method does nothing, caller is responsible for
 * closing underlying file himself.
 */
public class RandomAccessFileOutputStream extends OutputStream {

    private final RandomAccessFile file;

    /**
     * Constructor.
     *
     * @param file File to write to.
     */
    public RandomAccessFileOutputStream(RandomAccessFile file) {
        this.file = file;
    }

    @Override
    public void write(int b) throws IOException {
        file.write(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        file.write(b, off, len);
    }

    @Override
    public void close() {
        // Does nothing.
    }
}
