package store.common.value;

final class IntegerValue extends Value {

    private final long value;

    public IntegerValue(long value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.INTEGER;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
