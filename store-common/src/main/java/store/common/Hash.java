package store.common;

import static com.google.common.io.BaseEncoding.base16;
import java.util.Arrays;

public final class Hash {

    private final byte[] value;

    public Hash(byte[] value) {
        this.value = value;
    }

    public Hash(String encoded) {
        this(base16()
                .lowerCase()
                .decode(encoded.toLowerCase()));
    }

    public final byte[] value() {
        return Arrays.copyOf(value, value.length);
    }

    public final String encode() {
        return base16()
                .lowerCase()
                .encode(value);
    }

    @Override
    public final String toString() {
        return encode();
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        return Arrays.equals(value, Hash.class.cast(obj).value);
    }
}
