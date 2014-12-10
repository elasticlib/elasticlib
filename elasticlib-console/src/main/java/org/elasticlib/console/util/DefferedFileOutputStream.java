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
package org.elasticlib.console.util;

import java.io.IOException;
import java.io.OutputStream;
import static java.nio.file.Files.newOutputStream;
import java.nio.file.Path;
import java.util.Optional;

/**
 * A file output stream that create or truncate file in a lazy way, only when first byte is written.
 */
public class DefferedFileOutputStream extends OutputStream {

    private final Path path;
    private Optional<OutputStream> outputStream;

    /**
     * Constructor.
     *
     * @param path Path to write to.
     */
    public DefferedFileOutputStream(Path path) {
        this.path = path;
        outputStream = Optional.empty();
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
