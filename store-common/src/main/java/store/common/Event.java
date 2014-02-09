package store.common;

import java.util.Date;

public class Event {

    private final long seq;
    private final Hash hash;
    private final Date timestamp;
    private final Operation operation;

    private Event(EventBuilder builder) {
        this.seq = builder.seq;
        this.hash = builder.hash;
        this.timestamp = builder.timestamp;
        this.operation = builder.operation;
    }

    public long getSeq() {
        return seq;
    }

    public Hash getHash() {
        return hash;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "Event{" + "seq=" + seq +
                ", hash=" + hash +
                ", timestamp=" + timestamp.getTime() +
                ", operation=" + operation.name() + '}';
    }

    @Override
    public int hashCode() {
        int result = 5;
        result = 89 * result + (int) (this.seq ^ (this.seq >>> 32));
        result = 89 * result + hash.hashCode();
        result = 89 * result + timestamp.hashCode();
        result = 89 * result + operation.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Event other = (Event) obj;
        if (seq != other.seq) {
            return false;
        }
        if (!hash.equals(other.hash)) {
            return false;
        }
        if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        if (operation != other.operation) {
            return false;
        }
        return true;
    }

    public static class EventBuilder {

        private Long seq;
        private Hash hash;
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
