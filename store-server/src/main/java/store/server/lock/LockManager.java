package store.server.lock;

import store.common.Hash;
import store.server.table.Table;

public class LockManager {

    private static final int KEY_LENGTH = 1;
    private final Table<Segment> locks;

    public LockManager() {
        locks = new Table<Segment>(KEY_LENGTH) {
            @Override
            protected Segment initialValue() {
                return new Segment();
            }
        };
    }

    public boolean writeLock(Hash hash) {
        Segment segment = locks.get(hash);
        synchronized (segment) {
            return segment.writeLock(hash);
        }
    }

    public void writeUnlock(Hash hash) {
        Segment segment = locks.get(hash);
        synchronized (segment) {
            segment.writeUnlock(hash);
        }
    }

    public boolean readLock(Hash hash) {
        Segment segment = locks.get(hash);
        synchronized (segment) {
            return segment.readLock(hash);
        }
    }

    public void readUnlock(Hash hash) {
        Segment segment = locks.get(hash);
        synchronized (segment) {
            segment.readUnlock(hash);
        }
    }
}
