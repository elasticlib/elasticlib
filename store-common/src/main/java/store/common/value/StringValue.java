package store.common.value;

import static java.util.Objects.requireNonNull;

class StringValue extends Value {

    private final String value;

    public StringValue(String value) {
        this.value = requireNonNull(value);
    }

    @Override
    public ValueType type() {
        return ValueType.STRING;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
