package store.common.value;

import static java.util.Collections.unmodifiableMap;
import java.util.LinkedHashMap;
import java.util.Map;

class MapValue extends Value {

    private final Map<String, Value> value;

    public MapValue(Map<String, Value> value) {
        this.value = unmodifiableMap(new LinkedHashMap<>(value));
    }

    @Override
    public ValueType type() {
        return ValueType.OBJECT;
    }

    @Override
    public Map<String, Value> asMap() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
