package store.server.index;

import java.io.IOException;
import org.apache.lucene.store.IndexOutput;
import store.server.transaction.Output;

class TransactionalIndexOutput extends IndexOutput {

    private final Output output;
    private long position;

    public TransactionalIndexOutput(Output output) {
        this.output = output;
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    @Deprecated
    public void seek(long pos) {
        throw new UnsupportedOperationException(); // On croise les doigts...
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLength(long length) {
        // Au besoin on pourrait ici pré-allouer de l'espace disque
    }

    @Override
    public void writeByte(byte b) {
        output.write(b);
        position++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        output.write(b, offset, length);
        position += length;
    }

    @Override
    public void flush() throws IOException {
        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
