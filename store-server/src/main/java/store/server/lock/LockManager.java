package store.server.lock;

import store.common.Hash;
import store.server.Table;

public class LockManager {

    private static final int KEY_LENGTH = 1;
    private final Table<Segment> segments;

    public LockManager() {
        segments = new Table<Segment>(KEY_LENGTH) {
            @Override
            protected Segment initialValue(String key) {
                return new Segment();
            }
        };
    }

    public boolean writeLock(Hash hash) {
        return segments.get(hash).writeLock(hash);
    }

    public void writeUnlock(Hash hash) {
        segments.get(hash).writeUnlock(hash);
    }

    public boolean readLock(Hash hash) {
        return segments.get(hash).readLock(hash);
    }

    public void readUnlock(Hash hash) {
        segments.get(hash).readUnlock(hash);
    }
}
