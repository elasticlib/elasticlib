package store.lock;

import store.Index;
import store.hash.Hash;

public class LockManager {

    private static final int KEY_LENGTH = 1;
    private final Page[] locks;

    public LockManager() {
        locks = new Page[1 << (4 * KEY_LENGTH)];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Page();
        }
    }

    public boolean lock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            return locks[index].add(hash);
        }
    }

    public void unlock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            locks[index].remove(hash);
        }
    }

    private static String key(Hash hash) {
        return hash.encode()
                .substring(0, KEY_LENGTH);
    }
}
