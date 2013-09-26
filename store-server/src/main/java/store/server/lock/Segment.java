package store.server.lock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import store.common.Hash;

final class Segment {

    private final Map<Hash, Counter> read = new HashMap<>();
    private final Set<Hash> write = new HashSet<>();

    public synchronized boolean writeLock(Hash hash) {
        if (write.contains(hash) || read.containsKey(hash)) {
            return false;
        }
        return write.add(hash);
    }

    public synchronized void writeUnlock(Hash hash) {
        write.remove(hash);
    }

    public synchronized boolean readLock(Hash hash) {
        if (write.contains(hash)) {
            return false;
        }
        if (!read.containsKey(hash)) {
            read.put(hash, new Counter());
        }
        read.get(hash).increment();
        return true;
    }

    public synchronized void readUnlock(Hash hash) {
        Counter counter = read.get(hash);
        if (counter != null) {
            counter.decrement();
            if (counter.value() == 0) {
                read.remove(hash);
            }
        }
    }
}
