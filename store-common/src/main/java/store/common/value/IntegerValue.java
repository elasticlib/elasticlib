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
    public String toString() {
        return Integer.toString(value);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + type().hashCode();
        hash = 17 * hash + value;
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
        IntegerValue other = (IntegerValue) obj;
        return value == other.value;
    }
}
