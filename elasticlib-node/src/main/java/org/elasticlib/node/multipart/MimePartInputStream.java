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
package org.elasticlib.node.multipart;

import java.io.IOException;
import java.io.InputStream;
import org.jvnet.mimepull.MIMEPart;

/**
 * Wraps the MIME part inputStream. Provides a customized close() method which triggers MIME part closing, behind the
 * scenes. In fact, this is not very important because :<br>
 * - Entity inputStream is closed by the multipart.<br>
 * - MIME parts are not expected to create temporary files and therefore, they do not hold any resource.
 */
class MimePartInputStream extends InputStream {

    private final MIMEPart mimePart;
    private final InputStream delegate;

    public MimePartInputStream(MIMEPart mimePart) {
        this.mimePart = mimePart;
        delegate = mimePart.readOnce();
    }

    @Override
    public int read() throws IOException {
        return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            mimePart.close();
        }
    }
}
