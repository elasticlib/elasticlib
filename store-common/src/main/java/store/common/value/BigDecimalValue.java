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
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + type().hashCode();
        hash = 83 * hash + value.hashCode();
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
        BigDecimalValue other = (BigDecimalValue) obj;
        return value.equals(other.value);
    }
}
