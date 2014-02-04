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
    public String toString() {
        return "\"" + value + "\"";
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + type().hashCode();
        hash = 43 * hash + value.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StringValue other = (StringValue) obj;
        return value.equals(other.value);
    }
}
