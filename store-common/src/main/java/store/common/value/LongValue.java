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
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + type().hashCode();
        hash = 97 * hash + (int) (value ^ (value >>> 32));
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
        LongValue other = (LongValue) obj;
        return value == other.value;
    }
}
