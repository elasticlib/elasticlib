package store.client.http;

import java.io.IOException;
import java.io.InputStream;
import store.client.config.ClientConfig;
import store.client.display.Display;

/**
 * An input stream that logs read progression.
 */
class LoggingInputStream extends InputStream {

    private static final int EOF = -1;
    private final Display display;
    private final ClientConfig config;
    private final String task;
    private InputStream inputStream;
    private final long length;
    private long read;
    private int currentProgress;
    private boolean closed;

    public LoggingInputStream(Display display, ClientConfig config, String task, InputStream inputStream, long length) {
        this.display = display;
        this.config = config;
        this.task = task;
        this.inputStream = inputStream;
        this.length = length;
    }

    private void increment(long n) {
        read += n;
        int newProgress = (int) ((read * 100.0d) / length);
        if (newProgress != currentProgress) {
            currentProgress = newProgress;
            if (config.isDisplayProgress()) {
                display.print(task + " " + currentProgress + "%\r");
            }
        }
    }

    @Override
    public int read() throws IOException {
        int res = inputStream.read();
        if (res != EOF) {
            increment(1);
        }
        return res;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int res = inputStream.read(b, off, len);
        if (res != EOF) {
            increment(res);
        }
        return res;
    }

    @Override
    public long skip(long n) throws IOException {
        increment(n);
        return inputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return inputStream.available();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (config.isDisplayProgress()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < task.length() + " 100%".length(); i++) {
                    builder.append(" ");
                }
                builder.append('\r');
                display.print(builder.toString());
            }
            inputStream.close();
        }
    }
}
