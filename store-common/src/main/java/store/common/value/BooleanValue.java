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
    Object value() {
        return value;
    }
}
