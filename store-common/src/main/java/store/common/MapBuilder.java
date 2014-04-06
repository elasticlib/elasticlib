package store.common;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.Instant;
import store.common.hash.Hash;
import store.common.value.Value;

class MapBuilder {

    private final Map<String, Value> map = new LinkedHashMap<>();

    public MapBuilder put(String key, Hash value) {
        return put(key, Value.of(value));
    }

    public MapBuilder put(String key, byte[] value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, boolean value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, long value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, String value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, BigDecimal value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, Instant value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, Value value) {
        map.put(key, value);
        return this;
    }

    public MapBuilder put(String key, List<Value> value) {
        map.put(key, Value.of(value));
        return this;
    }

    public MapBuilder put(String key, Map<String, Value> value) {
        map.put(key, Value.of(value));
        return this;
    }

    public Map<String, Value> build() {
        return map;
    }
}
