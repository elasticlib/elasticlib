package store.server.table;

import store.common.Hash;
import static store.server.table.TableUtil.indexOf;
import static store.server.table.TableUtil.keyOf;

public abstract class Table<T> {

    private final int keyLength;
    private final Object[] buckets;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public Table(int keyLength) {
        this.keyLength = keyLength;
        buckets = new Object[1 << (4 * keyLength)];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = initialValue(keyOf(i, keyLength));
        }
    }

    protected abstract T initialValue(String key);

    @SuppressWarnings("unchecked")
    public T get(Hash hash) {
        int index = indexOf(hash.encode().substring(0, keyLength));
        return (T) buckets[index];
    }
}
