package store.common;

public final class Hash extends AbstractKey {

    public Hash(byte[] value) {
        super(value);
    }

    public Hash(String encoded) {
        super(encoded);
    }
}
