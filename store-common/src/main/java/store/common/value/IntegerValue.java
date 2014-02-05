package store.common.value;

class IntegerValue extends Value {

    private final int value;

    public IntegerValue(int value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.INTEGER;
    }

    @Override
    public int asInt() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
