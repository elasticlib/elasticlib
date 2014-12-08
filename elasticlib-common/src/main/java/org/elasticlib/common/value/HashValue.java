package org.elasticlib.common.value;

import static java.util.Objects.requireNonNull;
import org.elasticlib.common.hash.Hash;

final class HashValue extends Value {

    private final Hash value;

    HashValue(Hash value) {
        this.value = requireNonNull(value);
    }

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public Hash asHash() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
