package store.common;

import static com.google.common.io.BaseEncoding.base16;
import java.util.Arrays;

public abstract class AbstractKey {

    private final byte[] value;

    public AbstractKey(byte[] value) {
        this.value = value;
    }

    public AbstractKey(String encoded) {
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
        return Arrays.equals(value, AbstractKey.class.cast(obj).value);
    }
}