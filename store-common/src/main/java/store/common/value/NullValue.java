package store.common.value;

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
        int hash = 7;
        hash = 67 * hash + type().hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return obj.getClass() == NullValue.class;
    }

    @Override
    Object value() {
        throw new UnsupportedOperationException();
    }
}
