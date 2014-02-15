package store.common;

import com.google.common.base.Optional;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.TreeMap;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Represents a diff from a revision to another.
 */
public final class ContentInfoDiff {

    private final Hash from;
    private final Map<String, Value> diff;

    /**
     * Constructor.
     *
     * @param from Source revision hash.
     * @param diff Differences from source revision.
     */
    public ContentInfoDiff(Hash from, Map<String, Value> diff) {
        this.from = requireNonNull(from);
        this.diff = unmodifiableMap(new TreeMap<>(diff));
    }

    /**
     * @return Source revision hash.
     */
    public Hash getFrom() {
        return from;
    }

    /**
     * @return Differences from source revision.
     */
    public Map<String, Value> getDiff() {
        return diff;
    }

    /**
     * Compute diff between two revisions.
     *
     * @param from Source revision.
     * @param to Target revision.
     * @return A new ContentInfoDiff instance.
     */
    public static ContentInfoDiff of(ContentInfo from, ContentInfo to) {
        Map<String, Value> diff = new HashMap<>();
        for (Entry<String, Value> entry : to.getMetadata().entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            if (!Objects.equals(from.getMetadata().get(key), value)) {
                diff.put(key, value);
            }
        }
        for (String key : from.getMetadata().keySet()) {
            if (!to.getMetadata().keySet().contains(key)) {
                diff.put(key, Value.ofNull());
            }
        }
        return new ContentInfoDiff(from.getRev(), diff);
    }

    /**
     * Merge another diff with this one. This and that are expected to be based on same source revision. Returns a merge
     * diff if there is no conflict, or nothing otherwise.
     *
     * @param that Another diff.
     * @return An optional new resulting diff.
     */
    public Optional<ContentInfoDiff> merge(ContentInfoDiff that) {
        if (!that.from.equals(from)) {
            throw new IllegalArgumentException();
        }
        Map<String, Value> merge = new HashMap<>(diff);
        for (Entry<String, Value> entry : that.diff.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            if (merge.containsKey(key) && !merge.get(key).equals(value)) {
                return Optional.absent();
            }
            if (!merge.containsKey(key)) {
                merge.put(key, value);
            }
        }
        return Optional.of(new ContentInfoDiff(from, merge));
    }

    /**
     * Apply this diff to its source revision.
     *
     * @param info Actual source revision of this diff.
     * @return Resulting metadata.
     */
    public Map<String, Value> apply(ContentInfo info) {
        if (!info.getRev().equals(from)) {
            throw new IllegalArgumentException();
        }
        Map<String, Value> result = new HashMap<>(info.getMetadata());
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
