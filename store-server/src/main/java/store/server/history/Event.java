package store.server.history;

import java.util.Set;
import store.common.Hash;
import store.server.Uid;

public class Event {

    private final Hash hash;
    private final long timestamp;
    private final Operation operation;
    private final Set<Uid> uids;

    public Event(Hash hash, long timestamp, Operation operation, Set<Uid> uids) {
        this.hash = hash;
        this.timestamp = timestamp;
        this.operation = operation;
        this.uids = uids;
    }

    public Hash getHash() {
        return hash;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Operation getOperation() {
        return operation;
    }

    public Set<Uid> getUids() {
        return uids;
    }
}
