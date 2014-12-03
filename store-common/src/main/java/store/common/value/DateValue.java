package store.common.value;

import java.time.Instant;

final class DateValue extends Value {

    private final Instant value;

    public DateValue(Instant value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.DATE;
    }

    @Override
    public Instant asInstant() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
