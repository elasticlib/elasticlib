package store.server.index;

import org.apache.lucene.store.IndexInput;
import store.server.transaction.Input;

class TransactionalIndexInput extends IndexInput {

    private final long length;
    private final Input input;

    public TransactionalIndexInput(String resourceDescription, long length, Input input) {
        super(resourceDescription);
        this.length = length;
        this.input = input;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long getFilePointer() {
        return input.position();
    }

    @Override
    public void seek(long pos) {
        input.position(pos);
    }

    @Override
    public byte readByte() {
        return checkedCast(input.read());
    }

    private static byte checkedCast(int i) {
        byte b = (byte) i;
        if (i != b) {
            throw new RuntimeException();
        }
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
        int ret = input.read(b, offset, len);
        if (ret == -1 || ret > len) {
            throw new RuntimeException();
        }
        if (ret < len) {
            readBytes(b, offset + ret, len - ret);
        }
    }

    @Override
    public void close() {
        input.close();
    }
}
