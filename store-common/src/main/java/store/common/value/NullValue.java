package store.common.value;

import static java.util.Objects.hash;

final class NullValue extends Value {

    private static final NullValue INSTANCE = new NullValue();

    private NullValue() {
    }

    public static NullValue getInstance() {
        return INSTANCE;
    }

    @Override
    public ValueType type() {
        return ValueType.NULL;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public int hashCode() {
        return hash(type());
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    Object value() {
        throw new UnsupportedOperationException();
    }
}
