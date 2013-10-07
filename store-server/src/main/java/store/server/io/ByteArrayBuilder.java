package store.server.io;

import java.nio.ByteBuffer;
import java.util.Arrays;

class ByteArrayBuilder {

    private byte[] array;
    private int size;

    public ByteArrayBuilder() {
        this(32);
    }

    public ByteArrayBuilder(int capacity) {
        array = new byte[capacity];
        this.size = 0;
    }

    public ByteArrayBuilder append(byte b) {
        ensureCapacity(size + 1);
        array[size] = b;
        size++;
        return this;
    }

    public ByteArrayBuilder append(byte[] bytes) {
        ensureCapacity(size + bytes.length);
        System.arraycopy(bytes, 0, array, size, bytes.length);
        size += bytes.length;
        return this;
    }

    private void ensureCapacity(int capacity) {
        if (array.length < capacity) {
            int length = array.length;
            while (length < capacity) {
                length *= 2;
            }
            array = Arrays.copyOf(array, length);
        }
    }

    public byte[] build() {
        return Arrays.copyOf(array, size);
    }

    public byte[] prependSizeAndBuild() {
        return new ByteArrayBuilder(size + 4)
                .append(encodeSize())
                .append(build())
                .build();
    }

    private byte[] encodeSize() {
        return ByteBuffer.allocate(4)
                .putInt(size)
                .array();
    }
}
