package store.server.lock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import store.common.Hash;

final class Segment {

    private final Lock lock = new ReentrantLock(true);
    private final Condition readable = lock.newCondition();
    private final Condition writable = lock.newCondition();
    private final Map<Hash, Counter> read = new HashMap<>();
    private final Set<Hash> write = new HashSet<>();

    public void writeLock(Hash hash) {
        lock.lock();
        try {
            while (write.contains(hash) || read.containsKey(hash)) {
                writable.awaitUninterruptibly();
            }
            write.add(hash);

        } finally {
            lock.unlock();
        }
    }

    public void writeUnlock(Hash hash) {
        lock.lock();
        try {
            write.remove(hash);
            writable.signal();
            readable.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public void readLock(Hash hash) {
        lock.lock();
        try {
            while (write.contains(hash)) {
                readable.awaitUninterruptibly();
            }
            if (!read.containsKey(hash)) {
                read.put(hash, new Counter());
            }
            read.get(hash).increment();

        } finally {
            lock.unlock();
        }
    }

    public void readUnlock(Hash hash) {
        lock.lock();
        try {
            Counter counter = read.get(hash);
            if (counter != null) {
                counter.decrement();
                if (counter.value() == 0) {
                    read.remove(hash);
                }
            }
            writable.signal();
            readable.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            read.clear();
            write.clear();

        } finally {
            lock.unlock();
        }
    }
}
