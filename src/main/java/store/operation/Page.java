package store.operation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import store.hash.Hash;
import static store.operation.LockState.*;

final class Page {

    private final Set<Hash> puts = new HashSet<>();
    private final Map<Hash, Counter> gets = new HashMap<>();
    private final Set<Hash> deletes = new HashSet<>();

    public LockState putLock(Hash hash) {
        if (deletes.contains(hash)) {
            return DENIED;
        }
        return puts.add(hash) ? GRANTED : DENIED;
    }

    public void putUnlock(Hash hash) {
        puts.remove(hash);
    }

    public LockState getLock(Hash hash) {
        if (deletes.contains(hash)) {
            return DENIED;
        }
        if (!gets.containsKey(hash)) {
            gets.put(hash, new Counter());
        }
        gets.get(hash)
                .increment();
        return GRANTED;
    }

    public LockState getUnlock(Hash hash) {
        Counter counter = gets.get(hash);
        if (counter == null) {
            return deletes.contains(hash) ? ERASABLE : GRANTED;
        }
        counter.decrement();
        if (counter.value() == 0) {
            gets.remove(hash);
            if (deletes.contains(hash)) {
                return ERASABLE;
            }
        }
        return GRANTED;
    }

    public LockState deleteLock(Hash hash) {
        if (puts.contains(hash) || deletes.contains(hash)) {
            return DENIED;
        }
        deletes.add(hash);
        return gets.containsKey(hash) ? GRANTED : ERASABLE;
    }

    public void deleteUnlock(Hash hash) {
        deletes.remove(hash);
    }
}
