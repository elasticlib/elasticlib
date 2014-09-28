package store.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.SortedSet;
import org.joda.time.Instant;
import store.common.hash.Hash;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import static store.common.mappable.MappableUtil.putRevisions;
import static store.common.mappable.MappableUtil.revisions;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Represents an event in the history of a repository.
 */
public class Event implements Mappable {

    private static final String SEQ = "seq";
    private static final String CONTENT = "content";
    private static final String REVISIONS = "revisions";
    private static final String TIMESTAMP = "timestamp";
    private static final String OPERATION = "operation";
    private final long seq;
    private final Hash content;
    private final SortedSet<Hash> revisions;
    private final Instant timestamp;
    private final Operation operation;

    private Event(EventBuilder builder) {
        this.seq = builder.seq;
        this.content = requireNonNull(builder.content);
        this.revisions = unmodifiableSortedSet(builder.revisions);
        this.timestamp = requireNonNull(builder.timestamp);
        this.operation = requireNonNull(builder.operation);
    }

    /**
     * Provides this event sequence number.
     *
     * @return A long.
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Provides hash of the content associated with this event.
     *
     * @return A hash.
     */
    public Hash getContent() {
        return content;
    }

    /**
     * Provides head revisions hashes after this event.
     *
     * @return A sorted set of hashes.
     */
    public SortedSet<Hash> getRevisions() {
        return revisions;
    }

    /**
     * Provides local timestamp of this event.
     *
     * @return A date.
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Provides operation performed.
     *
     * @return An operation
     */
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(SEQ, seq)
                .put(CONTENT, content);

        return putRevisions(builder, revisions)
                .put(TIMESTAMP, timestamp)
                .put(OPERATION, operation.toString())
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static Event fromMap(Map<String, Value> map) {
        return new EventBuilder()
                .withSeq(map.get(SEQ).asLong())
                .withContent(map.get(CONTENT).asHash())
                .withRevisions(revisions(map))
                .withTimestamp(map.get(TIMESTAMP).asInstant())
                .withOperation(Operation.fromString(map.get(OPERATION).asString()))
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(SEQ, seq)
                .add(CONTENT, content)
                .add(REVISIONS, revisions)
                .add(TIMESTAMP, timestamp)
                .add(OPERATION, operation)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(seq, content, revisions, timestamp, operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Event)) {
            return false;
        }
        Event other = (Event) obj;
        return new EqualsBuilder()
                .append(seq, other.seq)
                .append(content, other.content)
                .append(revisions, other.revisions)
                .append(timestamp, other.timestamp)
                .append(operation, other.operation)
                .build();
    }

    /**
     * Builder.
     */
    public static class EventBuilder {

        private Long seq;
        private Hash content;
        private SortedSet<Hash> revisions;
        private Instant timestamp;
        private Operation operation;

        /**
         * Set sequence number.
         *
         * @param seq Sequence number.
         * @return this
         */
        public EventBuilder withSeq(long seq) {
            this.seq = seq;
            return this;
        }

        /**
         * Set content hash.
         *
         * @param hash Content hash.
         * @return this
         */
        public EventBuilder withContent(Hash hash) {
            this.content = hash;
            return this;
        }

        /**
         * Set head revisions hashes.
         *
         * @param revisions Head revisions hashes.
         * @return this
         */
        public EventBuilder withRevisions(SortedSet<Hash> revisions) {
            this.revisions = revisions;
            return this;
        }

        /**
         * Set timestamp.
         *
         * @param timestamp Timestamp.
         * @return this
         */
        public EventBuilder withTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Set operation.
         *
         * @param operation Operation.
         * @return this
         */
        public EventBuilder withOperation(Operation operation) {
            this.operation = operation;
            return this;
        }

        /**
         * Build.
         *
         * @return A new instance.
         */
        public Event build() {
            return new Event(this);
        }
    }
}
