/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.value;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;

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
     * @param value A hash.
     * @return A value of the supplied hash.
     */
    public static Value of(Hash value) {
        return new HashValue(value);
    }

    /**
     * @param value A GUID.
     * @return A value of the supplied GUID.
     */
    public static Value of(Guid value) {
        return new GuidValue(value);
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
     * @param value A long.
     * @return A value of the supplied long.
     */
    public static Value of(long value) {
        return new IntegerValue(value);
    }

    /**
     * @param value A big decimal.
     * @return A value of the supplied big decimal.
     */
    public static Value of(BigDecimal value) {
        return new DecimalValue(value);
    }

    /**
     * @param value A string.
     * @return A value of the supplied string.
     */
    public static Value of(String value) {
        return new StringValue(value);
    }

    /**
     * @param value An instant.
     * @return A value of the supplied instant.
     */
    public static Value of(Instant value) {
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
     * @return This value as a hash.
     * @throws UnsupportedOperationException If this value is actually not a hash.
     */
    public Hash asHash() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a GUID.
     * @throws UnsupportedOperationException If this value is actually not a GUID.
     */
    public Guid asGuid() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return This value as a byte array.
     * @throws UnsupportedOperationException If this value is actually not a byte array.
     */
    public byte[] asByteArray() {
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
     * @return This value as an instant.
     * @throws UnsupportedOperationException If this value is actually not an instant.
     */
    public Instant asInstant() {
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
