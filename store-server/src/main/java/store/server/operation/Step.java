package store.server.operation;

import store.common.Hash;
import store.server.Uid;

public final class Step {

    private final Uid uid;
    private final Hash hash;
    private final long timestamp;
    private final OpCode opCode;

    public Step(Uid uid, Hash hash, long timestamp, OpCode opCode) {
        this.uid = uid;
        this.hash = hash;
        this.timestamp = timestamp;
        this.opCode = opCode;
    }

    public Uid getUid() {
        return uid;
    }

    public Hash getHash() {
        return hash;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public OpCode getOpCode() {
        return opCode;
    }
}
