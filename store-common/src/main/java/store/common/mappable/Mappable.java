package store.common.mappable;

import java.util.Map;
import store.common.value.Value;

/**
 * Represents a type that can be converted to a map of values.
 */
public interface Mappable {

    /**
     * Writes this instance to a map of values.
     *
     * @return A map of values.
     */
    Map<String, Value> toMap();
}
