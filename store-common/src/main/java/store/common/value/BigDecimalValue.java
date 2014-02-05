package store.common.value;

import java.math.BigDecimal;
import static java.util.Objects.requireNonNull;

class BigDecimalValue extends Value {

    private final BigDecimal value;

    public BigDecimalValue(BigDecimal value) {
        this.value = requireNonNull(value);
    }

    @Override
    public ValueType type() {
        return ValueType.BIG_DECIMAL;
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
