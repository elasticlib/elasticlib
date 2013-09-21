package store.server.table;

import java.util.concurrent.atomic.AtomicReferenceArray;
import store.common.hash.Hash;
import static store.server.table.TableUtil.indexOf;

public abstract class AtomicTable<T> {

    private final int keyLength;
    private final AtomicReferenceArray<T> buckets;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public AtomicTable(int keyLength) {
        this.keyLength = keyLength;
        buckets = new AtomicReferenceArray<>(1 << (4 * keyLength));
        for (int i = 0; i < buckets.length(); i++) {
            buckets.set(i, initialValue());
        }
    }

    protected abstract T initialValue();

    public T get(Hash hash) {
        return buckets.get(index(hash));
    }

    public void set(Hash hash, T value) {
        buckets.set(index(hash), value);
    }

    public boolean compareAndSet(Hash hash, T expect, T update) {
        return buckets.compareAndSet(index(hash), expect, update);
    }

    private int index(Hash hash) {
        return indexOf(hash.encode()
                .substring(0, keyLength));
    }
}
