package store.server.transaction;

final class EmptyInput extends Input {

    private static final int EOF = -1;
    public static final Input INSTANCE = new EmptyInput();

    private EmptyInput() {
        super(null);
    }

    @Override
    public int read() {
        return EOF;
    }

    @Override
    public int read(byte[] bytes) {
        return EOF;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        return EOF;
    }

    @Override
    public long skip(long n) {
        return 0;
    }

    @Override
    public int available() {
        return 0;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void position(long n) {
        if (n != 0) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void close() {
    }
}
