package store.server.operation;

import store.common.hash.Hash;
import store.server.table.Table;

class Locks {

    private static final int KEY_LENGTH = 1;
    private final Table<Page> locks;

    public Locks() {
        locks = new Table<Page>(KEY_LENGTH) {
            @Override
            protected Page initialValue() {
                return new Page();
            }
        };
    }

    public LockState putLock(Hash hash) {
        Page page = locks.get(hash);
        synchronized (page) {
            return page.putLock(hash);
        }
    }

    public void putUnlock(Hash hash) {
        Page page = locks.get(hash);
        synchronized (page) {
            page.putUnlock(hash);
        }
    }

    public LockState getLock(Hash hash) {
        Page page = locks.get(hash);
        synchronized (page) {
            return page.getLock(hash);
        }
    }

    public LockState getUnlock(Hash hash) {
        Page page = locks.get(hash);
        synchronized (page) {
            return page.getUnlock(hash);
        }
    }

    public LockState deleteLock(Hash hash) {
        Page page = locks.get(hash);
        synchronized (page) {
            return page.deleteLock(hash);
        }
    }

    public void deleteUnlock(Hash hash) {
        Page page = locks.get(hash);
        synchronized (page) {
            page.deleteUnlock(hash);
        }
    }
}
