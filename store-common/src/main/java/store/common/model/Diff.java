package store.common.model;

import com.google.common.base.Optional;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Represents a diff from a metadata set to another one.
 */
public final class Diff {

    private final Map<String, Value> diff;

    /**
     * Constructor.
     *
     * @param diff Differences from source metadata set.
     */
    public Diff(Map<String, Value> diff) {
        this.diff = unmodifiableMap(new TreeMap<>(diff));
    }

    /**
     * @return this Diff as a map.
     */
    public Map<String, Value> getDiff() {
        return diff;
    }

    /**
     * Compute diff between two sets of metadata.
     *
     * @param from Source metadata set.
     * @param to Target metadata set.
     * @return A new Diff instance.
     */
    public static Diff diff(Map<String, Value> from, Map<String, Value> to) {
        Map<String, Value> diff = new HashMap<>();
        for (Entry<String, Value> entry : to.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            if (!Objects.equals(from.get(key), value)) {
                diff.put(key, value);
            }
        }
        for (String key : from.keySet()) {
            if (!to.keySet().contains(key)) {
                diff.put(key, Value.ofNull());
            }
        }
        return new Diff(diff);
    }

    /**
     * Merge two diffs. Returns a merge diff if there is no conflict, or nothing otherwise.
     *
     * @param left First diff.
     * @param right Second diff.
     * @return An optional new resulting diff.
     */
    public static Optional<Diff> mergeDiff(Diff left, Diff right) {
        Map<String, Value> merge = new HashMap<>(left.diff);
        for (Entry<String, Value> entry : right.diff.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            if (merge.containsKey(key) && !merge.get(key).equals(value)) {
                return Optional.absent();
            }
            if (!merge.containsKey(key)) {
                merge.put(key, value);
            }
        }
        return Optional.of(new Diff(merge));
    }

    /**
     * Apply this diff to a metadata set.
     *
     * @param source A metadata set.
     * @return Resulting metadata.
     */
    public Map<String, Value> apply(Map<String, Value> source) {
        Map<String, Value> result = new TreeMap<>(source);
        for (Entry<String, Value> entry : diff.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            if (value.type() == ValueType.NULL) {
                result.remove(key);
            } else {
                result.put(key, value);
            }
        }
        return result;
    }
}
