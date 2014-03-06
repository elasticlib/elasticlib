package store.common;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Date;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.SortedSet;

public class Event {

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

    public long getSeq() {
        return seq;
    }

    public Hash getHash() {
        return hash;
    }

    public SortedSet<Hash> getHead() {
        return head;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("seq", seq)
                .add("hash", hash)
                .add("head", head)
                .add("timestamp", timestamp)
                .add("operation", operation)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(seq, hash, head, timestamp, operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Event other = (Event) obj;
        return seq == other.seq &&
                hash.equals(other.hash) &&
                head.equals(other.head) &&
                timestamp.equals(other.timestamp) &&
                operation == other.operation;
    }

    public static class EventBuilder {

        private Long seq;
        private Hash hash;
        private SortedSet<Hash> head;
        private Date timestamp;
        private Operation operation;

        public EventBuilder withSeq(long seq) {
            this.seq = seq;
            return this;
        }

        public EventBuilder withHash(Hash hash) {
            this.hash = hash;
            return this;
        }

        public EventBuilder withHead(SortedSet<Hash> head) {
            this.head = head;
            return this;
        }

        public EventBuilder withTimestamp(Date timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public EventBuilder withOperation(Operation operation) {
            this.operation = operation;
            return this;
        }

        public Event build() {
            return new Event(this);
        }
    }
}
