package store.server.index;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

public class EmptyInput extends IndexInput {

    public EmptyInput(String resourceDescription) {
        super(resourceDescription);
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public long getFilePointer() {
        return 0L;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != 0) {
            throw new IOException("Input is empty !");
        }
    }

    @Override
    public byte readByte() throws IOException {
        throw new IOException("Input is empty !");
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        throw new IOException("Input is empty !");
    }

    @Override
    public void close() {
    }
}
