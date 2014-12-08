package org.elasticlib.common.bson;

import static com.google.common.base.Charsets.UTF_8;
import java.nio.ByteBuffer;
import java.util.Arrays;
import static org.elasticlib.common.bson.BinaryConstants.NULL_BYTE;

class ByteArrayReader {

    private final byte[] bytes;
    private int position;

    public ByteArrayReader(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        position = 0;
    }

    public byte[] readByteArray(int length) {
        byte[] value = Arrays.copyOfRange(bytes, position, position + length);
        position += length;
        return value;
    }

    public byte readByte() {
        byte b = bytes[position];
        position++;
        return b;
    }

    public int readInt() {
        int value = ByteBuffer.wrap(bytes, position, 4)
                .getInt();
        position += 4;
        return value;
    }

    public long readLong() {
        long value = ByteBuffer.wrap(bytes, position, 8)
                .getLong();
        position += 8;
        return value;
    }

    public String readString(int length) {
        String str = new String(bytes, position, length, UTF_8);
        position += length;
        return str;
    }

    public String readNullTerminatedString() {
        for (int i = position; i < bytes.length; i++) {
            if (bytes[i] == NULL_BYTE) {
                String str = new String(bytes, position, i - position, UTF_8);
                position = i + 1;
                return str;
            }
        }
        throw new IllegalStateException();
    }

    public int position() {
        return position;
    }

    public int length() {
        return bytes.length;
    }
}
