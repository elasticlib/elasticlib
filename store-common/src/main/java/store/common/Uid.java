package store.common;

import java.security.SecureRandom;
import store.common.AbstractKey;

public final class Uid extends AbstractKey {

    private static final SecureRandom numberGenerator = new SecureRandom();

    public Uid(byte[] value) {
        super(value);
    }

    public Uid(String encoded) {
        super(encoded);
    }

    public static Uid random() {
        byte[] randomBytes = new byte[16];
        numberGenerator.nextBytes(randomBytes);
        return new Uid(randomBytes);
    }
}
