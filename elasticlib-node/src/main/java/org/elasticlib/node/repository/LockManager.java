package org.elasticlib.node.repository;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.hash.Hash;

/**
 * Provides shared and exclusive locks on hashes.
 */
class LockManager {

    private final Lock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition();
    private final Condition writable = lock.newCondition();
    private final Set<Hash> write = new HashSet<>();
    private final Map<Hash, Counter> read = new HashMap<>();
    private boolean closed;

    public void close() {
        lock.lock();
        try {
            closed = true;
            write.clear();
            read.clear();
            writable.signalAll();
            readable.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public void writeLock(Hash hash) {
        lock.lock();
        try {
            while (write.contains(hash) || read.containsKey(hash)) {
                writable.awaitUninterruptibly();
            }
            if (closed) {
                throw new RepositoryClosedException();
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
            if (closed) {
                throw new RepositoryClosedException();
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

    private static class Counter {

        private int value;

        public void increment() {
            value++;
        }

        public void decrement() {
            value--;
        }

        public int value() {
            return value;
        }
    }
}
