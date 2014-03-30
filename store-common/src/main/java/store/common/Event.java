package store.common;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Date;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.SortedSet;
import static store.common.MappableUtil.fromList;
import static store.common.MappableUtil.toList;
import store.common.value.Value;

/**
 * Represents an event in the history of a repository.
 */
public class Event implements Mappable {

    private static final String SEQ = "seq";
    private static final String HASH = "hash";
    private static final String HEAD = "head";
    private static final String TIMESTAMP = "timestamp";
    private static final String OPERATION = "operation";
    private final long seq;
    private final Hash hash;
    private final SortedSet<Hash> head;
    private final Date timestamp;
    private final Operation operation;

    private Event(EventBuilder builder) {
        this.seq = builder.seq;
        this.hash = requireNonNull(builder.hash);
        this.head = unmodifiableSortedSet(builder.head);
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
    public Hash getHash() {
        return hash;
    }

    /**
     * Provides head of info after this event.
     *
     * @return A sorted set of revisions.
     */
    public SortedSet<Hash> getHead() {
        return head;
    }

    /**
     * Provides local timestamp of this event.
     *
     * @return A date.
     */
    public Date getTimestamp() {
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
        return new MapBuilder()
                .put(SEQ, seq)
                .put(HASH, hash)
                .put(HEAD, toList(head))
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
                .withHash(new Hash(map.get(HASH).asByteArray()))
                .withHead(fromList(map.get(HEAD).asList()))
                .withTimestamp(map.get(TIMESTAMP).asDate())
                .withOperation(Operation.fromString(map.get(OPERATION).asString()))
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(SEQ, seq)
                .add(HASH, hash)
                .add(HEAD, head)
                .add(TIMESTAMP, timestamp)
                .add(OPERATION, operation)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(seq, hash, head, timestamp, operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Event)) {
            return false;
        }
        Event other = (Event) obj;
        return new EqualsBuilder()
                .append(seq, other.seq)
                .append(hash, other.hash)
                .append(head, other.head)
                .append(timestamp, other.timestamp)
                .append(operation, other.operation)
                .build();
    }

    /**
     * Builder.
     */
    public static class EventBuilder {

        private Long seq;
        private Hash hash;
        private SortedSet<Hash> head;
        private Date timestamp;
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
         * Set hash.
         *
         * @param hash Hash.
         * @return this
         */
        public EventBuilder withHash(Hash hash) {
            this.hash = hash;
            return this;
        }

        /**
         * Set head.
         *
         * @param head Head.
         * @return this
         */
        public EventBuilder withHead(SortedSet<Hash> head) {
            this.head = head;
            return this;
        }

        /**
         * Set timestamp.
         *
         * @param timestamp Timestamp.
         * @return this
         */
        public EventBuilder withTimestamp(Date timestamp) {
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
