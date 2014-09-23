package store.common.mappable;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import store.common.hash.Hash;
import store.common.value.Value;

/**
 * Utilities to deal with mappables.
 */
public final class MappableUtil {

    private static final String REVISION = "revision";
    private static final String REVISIONS = "revisions";

    private MappableUtil() {
    }

    /**
     * Converts supplied set of hashes to a list of values.
     *
     * @param hashes A set of hashes.
     * @return Corresponding list of values.
     */
    public static List<Value> toList(SortedSet<Hash> hashes) {
        List<Value> values = new ArrayList<>();
        for (Hash hash : hashes) {
            values.add(Value.of(hash));
        }
        return values;
    }

    /**
     * Extracts a set of hashes from supplied list of values. All values are expected to be hash ones.
     *
     * @param values A list of values.
     * @return Corresponding set of hashes
     */
    public static SortedSet<Hash> fromList(List<Value> values) {
        SortedSet<Hash> hashes = new TreeSet<>();
        for (Value value : values) {
            hashes.add(value.asHash());
        }
        return hashes;
    }

    /**
     * Adds supplied revisions to builder, with adequate key.
     *
     * @param builder A map builder.
     * @param revisions Revisions to add.
     * @return Supplied builder instance.
     */
    public static MapBuilder putRevisions(MapBuilder builder, SortedSet<Hash> revisions) {
        if (revisions.size() == 1) {
            return builder.put(REVISION, revisions.first());
        }
        return builder.put(REVISIONS, toList(revisions));
    }

    /**
     * Extracts a set of revisions from supplied map of values.
     *
     * @param values A map of values.
     * @return A set of revision hashes.
     */
    public static SortedSet<Hash> revisions(Map<String, Value> values) {
        if (values.containsKey(REVISION)) {
            SortedSet<Hash> revisions = new TreeSet<>();
            revisions.add(values.get(REVISION).asHash());
            return revisions;

        } else {
            return fromList(values.get(REVISIONS).asList());
        }
    }

    /**
     * Converts supplied map of values to a {@link Mappable} instance.
     *
     * @param <T> Actual class to convert to.
     * @param values A map of values.
     * @param clazz Actual class to convert to.
     * @return A new instance of supplied class.
     */
    public static <T extends Mappable> T fromMap(Map<String, Value> values, Class<T> clazz) {
        try {
            Method method = clazz.getMethod("fromMap", Map.class);
            return clazz.cast(method.invoke(null, values));

        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
