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
        return ValueType.MAP;
    }

    @Override
    public Map<String, Value> asMap() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + type().hashCode();
        hash = 19 * hash + value.hashCode();
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
        MapValue other = (MapValue) obj;
        return value.equals(other.value);
    }
}
