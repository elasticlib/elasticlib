package store.common.value;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a dynamically typed value. Immutable.
 */
public abstract class Value {

    /**
     * @return The null value.
     */
    public static Value ofNull() {
        return NullValue.getInstance();
    }

    /**
     * @param value A byte array.
     * @return A value of the supplied byte array.
     */
    public static Value of(byte[] value) {
        return new ByteArrayValue(value);
    }

    /**
     * @param value A boolean.
     * @return A value of the supplied boolean.
     */
    public static Value of(boolean value) {
        return BooleanValue.of(value);
    }

    /**
     * @param value A byte.
     * @return A value of the supplied byte.
     */
    public static Value of(byte value) {
        return new ByteValue(value);
    }

    /**
     * @param value An integer.
     * @return A value of the supplied integer.
     */
    public static Value of(int value) {
        return new IntegerValue(value);
    }

    /**
     * @param value A long.
     * @return A value of the supplied long.
     */
    public static Value of(long value) {
        return new LongValue(value);
    }

    /**
     * @param value A big decimal.
     * @return A value of the supplied big decimal.
     */
    public static Value of(BigDecimal value) {
        return new BigDecimalValue(value);
    }

    /**
     * @param value A string.
     * @return A value of the supplied string.
     */
    public static Value of(String value) {
        return new StringValue(value);
    }

    /**
     * @param value A date.
     * @return A value of the supplied date.
     */
    public static Value of(Date value) {
        return new DateValue(value);
    }

    /**
     * @param value A map of values.
     * @return A value of the supplied map.
     */
    public static Value of(Map<String, Value> value) {
        return new MapValue(value);
    }

    /**
     * @param value A list of values.
     * @return A value of the supplied list.
     */
    public static Value of(List<Value> value) {
        return new ListValue(value);
    }

    /**
     * @return The actual type of this value.
     */
    public abstract ValueType type();

    /**
     * @return This value as a byte array.
     * @throws UnsupportedOperationException If this value is actually not a byte array.
     */
    public byte[] asByteArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a base 16 encoded String.
     * @throws UnsupportedOperationException If this value is actually not a byte nor a byte array.
     */
    public String asHexadecimalString() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a byte array.
     * @throws UnsupportedOperationException If this value is actually not a boolean.
     */
    public boolean asBoolean() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a byte.
     * @throws UnsupportedOperationException If this value is actually not a byte.
     */
    public byte asByte() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as an integer.
     * @throws UnsupportedOperationException If this value is actually not an integer.
     */
    public int asInt() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a long.
     * @throws UnsupportedOperationException If this value is actually not a long.
     */
    public long asLong() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a big decimal.
     * @throws UnsupportedOperationException If this value is actually not a big decimal.
     */
    public BigDecimal asBigDecimal() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a string.
     * @throws UnsupportedOperationException If this value is actually not a string.
     */
    public String asString() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a date.
     * @throws UnsupportedOperationException If this value is actually not a date.
     */
    public Date asDate() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a map of values.
     * @throws UnsupportedOperationException If this value is actually not a map.
     */
    public Map<String, Value> asMap() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a list of values.
     * @throws UnsupportedOperationException If this value is actually not a list.
     */
    public List<Value> asList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return value().toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(type(), value());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Value)) {
            return false;
        }
        Value other = (Value) obj;
        return value().equals(other.value());
    }

    abstract Object value();
}
