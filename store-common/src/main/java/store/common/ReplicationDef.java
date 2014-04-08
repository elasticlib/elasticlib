package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.value.Value;

/**
 * Defines a replication.
 */
public final class ReplicationDef implements Mappable {

    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private final String source;
    private final String destination;

    /**
     * Constructor.
     *
     * @param source Source repository.
     * @param destination Destination repository.
     */
    public ReplicationDef(String source, String destination) {
        this.source = requireNonNull(source);
        this.destination = requireNonNull(destination);
    }

    /**
     * @return The source repository.
     */
    public String getSource() {
        return source;
    }

    /**
     * @return The destination repository.
     */
    public String getDestination() {
        return destination;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(SOURCE, source)
                .put(DESTINATION, destination)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ReplicationDef fromMap(Map<String, Value> map) {
        return new ReplicationDef(map.get(SOURCE).asString(),
                                  map.get(DESTINATION).asString());
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(SOURCE, source)
                .add(DESTINATION, destination)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(source, destination);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReplicationDef)) {
            return false;
        }
        ReplicationDef other = (ReplicationDef) obj;
        return new EqualsBuilder()
                .append(source, other.source)
                .append(destination, other.destination)
                .build();
    }
}
