package store.common;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.joda.time.Instant;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.value.Value;

/**
 * Support class for building maps of values.
 */
public class MapBuilder {

    private final Map<String, Value> map = new LinkedHashMap<>();

    /**
     * Put a hash entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Hash value) {
        return put(key, Value.of(value));
    }

    /**
     * Put a Guid entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Guid value) {
        return put(key, Value.of(value));
    }

    /**
     * Put a byte array entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, byte[] value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a boolean entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, boolean value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a long entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, long value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a string entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, String value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a big decimal entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, BigDecimal value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put an instant entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Instant value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a value entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Value value) {
        map.put(key, value);
        return this;
    }

    /**
     * Put a list entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, List<Value> value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a map entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Map<String, Value> value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a set of entries in map to build.
     *
     * @param entries Entries to put.
     * @return This builder.
     */
    public MapBuilder putAll(Map<String, Value> entries) {
        for (Entry<String, Value> entry : entries.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    /**
     * Build map.
     *
     * @return A map of values.
     */
    public Map<String, Value> build() {
        return map;
    }
}
