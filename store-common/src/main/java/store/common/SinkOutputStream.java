package store.common;

import java.io.OutputStream;

public class SinkOutputStream extends OutputStream {

    private static final SinkOutputStream INSTANCE = new SinkOutputStream();

    private SinkOutputStream() {
    }

    public static OutputStream sink() {
        return INSTANCE;
    }

    @Override
    public void write(byte[] b, int off, int len) {
    }

    @Override
    public void write(int b) {
    }
}
