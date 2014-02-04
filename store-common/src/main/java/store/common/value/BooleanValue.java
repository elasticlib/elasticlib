package store.common.value;

final class BooleanValue extends Value {

    private static final BooleanValue TRUE = new BooleanValue(true);
    private static final BooleanValue FALSE = new BooleanValue(false);
    private final boolean value;

    private BooleanValue(boolean value) {
        this.value = value;
    }

    public static Value of(boolean value) {
        return value ? TRUE : FALSE;
    }

    @Override
    public ValueType type() {
        return ValueType.BOOLEAN;
    }

    @Override
    public boolean asBoolean() {
        return value;
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + type().hashCode();
        hash = 67 * hash + (value ? 1 : 0);
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
        BooleanValue other = (BooleanValue) obj;
        return value == other.value;
    }
}
