package store.operation;

import store.Index;
import store.hash.Hash;

class Locks {

    private static final int KEY_LENGTH = 1;
    private final Page[] locks;

    public Locks() {
        locks = new Page[1 << (4 * KEY_LENGTH)];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Page();
        }
    }

    public LockState putLock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            return locks[index].putLock(hash);
        }
    }

    public void putUnlock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            locks[index].putUnlock(hash);
        }
    }

    public LockState getLock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            return locks[index].getLock(hash);
        }
    }

    public LockState getUnlock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            return locks[index].getUnlock(hash);
        }
    }

    public LockState deleteLock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            return locks[index].deleteLock(hash);
        }
    }

    public void deleteUnlock(Hash hash) {
        int index = Index.of(key(hash));
        synchronized (locks[index]) {
            locks[index].deleteUnlock(hash);
        }
    }

    private static String key(Hash hash) {
        return hash.encode()
                .substring(0, KEY_LENGTH);
    }
}
