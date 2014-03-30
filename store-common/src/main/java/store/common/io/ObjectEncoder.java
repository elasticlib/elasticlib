package store.common.io;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static store.common.io.BinaryConstants.*;
import static store.common.io.Encoding.encodeKey;
import static store.common.io.Encoding.encodeValue;
import store.common.value.Value;

/**
 * A JSON-like binary encoder with a fluent API.
 * <p>
 * Serialize a sequence of key-value pairs into a JSON-like binary document. Produced format supports the embedding of
 * documents and arrays within other documents and arrays. It also contains extensions that allow representation of data
 * types that are not part of the JSON spec.
 *
 * @see ObjectDecoder
 */
public final class ObjectEncoder {

    ByteArrayBuilder arrayBuilder = new ByteArrayBuilder();

    /**
     * Add a null value to the binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @return This encoder instance.
     */
    public ObjectEncoder putNull(String key) {
        put(key, Value.ofNull());
        return this;
    }

    /**
     * Add a byte array to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, byte[] value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a boolean to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, boolean value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a byte to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, byte value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add an integer to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, int value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a long to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, long value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a string to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, String value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a big decimal to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, BigDecimal value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a date to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, Date value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a map to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, Map<String, Value> value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a list to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, List<Value> value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a value to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, Value value) {
        arrayBuilder.append(encodeType(value.type()))
                .append(encodeKey(key))
                .append(encodeValue(value));
        return this;
    }

    /**
     * Add a map of values to binary structure to build.
     *
     * @param map A map of values.
     * @return This encoder instance.
     */
    public ObjectEncoder put(Map<String, Value> map) {
        for (Entry<String, Value> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    /**
     * Build the binary structure.
     *
     * @return Built structure as a byte array.
     */
    public byte[] build() {
        return arrayBuilder.prependSizeAndBuild();
    }
}
