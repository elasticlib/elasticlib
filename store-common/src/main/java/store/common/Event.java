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

    public static EventBuilder event() {
        return new EventBuilder();
    }

    public static class EventBuilder {

        private Long seq;
        private Hash hash;
        private Date timestamp;
        private Operation operation;

        private EventBuilder() {
        }

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
