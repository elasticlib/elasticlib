package store.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.EnumMap;
import java.util.Map;
import static java.util.Objects.hash;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Holds statistics about a repository.
 */
public final class RepositoryStats implements Mappable {

    private static final String CREATIONS = "creations";
    private static final String UPDATES = "updates";
    private static final String DELETIONS = "deletions";
    private static final String OPERATIONS = "operations";
    private static final String METADATA = "metadata";
    private final long creations;
    private final long updates;
    private final long deletions;
    private final Map<String, Long> metadataCounts;

    /**
     * Constructor.
     *
     * @param creations The creations attribute.
     * @param updates The updates attribute.
     * @param deletions The deletions attribute.
     * @param metadataCounts The metadataCounts attribute.
     */
    public RepositoryStats(long creations, long updates, long deletions, Map<String, Long> metadataCounts) {
        this.creations = creations;
        this.updates = updates;
        this.deletions = deletions;
        this.metadataCounts = ImmutableMap.copyOf(metadataCounts);
    }

    /**
     * @return The total number of created contents in this repository.
     */
    public long getCreations() {
        return creations;
    }

    /**
     * @return The total number of updates in this repository.
     */
    public long getUpdates() {
        return updates;
    }

    /**
     * @return The total number of deleted contents in this repository.
     */
    public long getDeletions() {
        return deletions;
    }

    /**
     * @return A mapping of the number of contents per metadata key.
     */
    public Map<String, Long> getMetadataCounts() {
        return metadataCounts;
    }

    @Override
    public Map<String, Value> toMap() {
        Map<String, Value> metadata = Maps.transformValues(metadataCounts, input -> Value.of(input));
        Map<String, Value> operations = ImmutableMap.of(Operation.CREATE.toString(), Value.of(creations),
                                                        Operation.UPDATE.toString(), Value.of(updates),
                                                        Operation.DELETE.toString(), Value.of(deletions));

        return new MapBuilder()
                .put(OPERATIONS, operations)
                .put(METADATA, metadata)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static RepositoryStats fromMap(Map<String, Value> map) {
        Map<String, Long> metadata = Maps.transformValues(map.get(METADATA).asMap(), input -> input.asLong());
        Map<Operation, Long> operations = new EnumMap<>(Operation.class);
        map.get(OPERATIONS)
                .asMap()
                .entrySet()
                .stream()
                .forEach(entry -> operations.put(Operation.fromString(entry.getKey()), entry.getValue().asLong()));

        return new RepositoryStats(operations.get(Operation.CREATE),
                                   operations.get(Operation.UPDATE),
                                   operations.get(Operation.DELETE),
                                   metadata);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(CREATIONS, creations)
                .add(UPDATES, updates)
                .add(DELETIONS, deletions)
                .add(METADATA, metadataCounts)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(creations, updates, deletions, metadataCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RepositoryStats)) {
            return false;
        }
        RepositoryStats other = (RepositoryStats) obj;
        return new EqualsBuilder()
                .append(creations, other.creations)
                .append(updates, other.updates)
                .append(deletions, other.deletions)
                .append(metadataCounts, other.metadataCounts)
                .build();
    }
}
