package store.common;

import java.io.OutputStream;

/**
 * An output stream that actually does nothing.
 */
public class SinkOutputStream extends OutputStream {

    private static final SinkOutputStream INSTANCE = new SinkOutputStream();

    private SinkOutputStream() {
    }

    /**
     * @return the singleton instance.
     */
    public static OutputStream sink() {
        return INSTANCE;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        // Actually does nothing.
    }

    @Override
    public void write(int b) {
        // Actually does nothing.
    }
}
