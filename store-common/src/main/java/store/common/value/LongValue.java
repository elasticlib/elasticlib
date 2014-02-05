package store.common.value;

class LongValue extends Value {

    private final long value;

    public LongValue(long value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.LONG;
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
