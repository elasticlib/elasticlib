package store.common.value;

import static java.util.Objects.requireNonNull;
import store.common.hash.Guid;

final class GuidValue extends Value {

    private final Guid value;

    GuidValue(Guid value) {
        this.value = requireNonNull(value);
    }

    @Override
    public ValueType type() {
        return ValueType.GUID;
    }

    @Override
    public Guid asGuid() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
