package store.server.table;

import store.common.hash.Hash;
import static store.server.table.TableUtil.indexOf;

public abstract class Table<T> {

    private final int keyLength;
    private final Object[] buckets;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public Table(int keyLength) {
        this.keyLength = keyLength;
        buckets = new Object[1 << (4 * keyLength)];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = initialValue();
        }
    }

    protected abstract T initialValue();

    @SuppressWarnings("unchecked")
    public T get(Hash hash) {
        int index = indexOf(hash.encode().substring(0, keyLength));
        return (T) buckets[index];
    }
}
