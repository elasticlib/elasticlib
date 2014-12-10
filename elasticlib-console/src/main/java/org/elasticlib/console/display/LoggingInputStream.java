/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.console.display;

import java.io.IOException;
import java.io.InputStream;
import org.elasticlib.console.config.ConsoleConfig;

/**
 * An input stream that prints read progression.
 */
public class LoggingInputStream extends InputStream {

    private static final int EOF = -1;
    private final Display display;
    private final ConsoleConfig config;
    private final String task;
    private final InputStream inputStream;
    private final long length;
    private long read;
    private int currentProgress;
    private boolean closed;

    /**
     * Constructor.
     *
     * @param display Display.
     * @param config Config.
     * @param task Task description to print.
     * @param inputStream Underlying input-stream.
     * @param length Expected total length to read.
     */
    public LoggingInputStream(Display display, ConsoleConfig config, String task, InputStream inputStream, long length) {
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
            log(task + " " + currentProgress + "%\r");
        }
    }

    private void log(String message) {
        if (config.isDisplayProgress()) {
            display.print(message);
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
        if (closed) {
            return;
        }
        closed = true;

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < task.length() + " 100%".length(); i++) {
            builder.append(" ");
        }
        builder.append('\r');
        log(builder.toString());

        inputStream.close();
    }
}
