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
