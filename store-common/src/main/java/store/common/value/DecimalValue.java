package store.common.value;

import java.math.BigDecimal;
import static java.util.Objects.requireNonNull;

final class DecimalValue extends Value {

    private final BigDecimal value;

    public DecimalValue(BigDecimal value) {
        this.value = requireNonNull(value);
    }

    @Override
    public ValueType type() {
        return ValueType.DECIMAL;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value;
    }

    @Override
    public String toString() {
        return value.toPlainString();
    }

    @Override
    Object value() {
        return value;
    }
}
