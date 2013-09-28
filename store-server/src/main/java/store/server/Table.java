package store.server;

import java.util.HashSet;
import java.util.Set;
import store.common.Hash;

public abstract class Table<T> {

    private final int keyLength;
    private final Object[] buckets;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public Table(int keyLength) {
        this.keyLength = keyLength;
        buckets = new Object[size(keyLength)];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = initialValue(keyOf(i, keyLength));
        }
    }

    protected abstract T initialValue(String key);

    @SuppressWarnings("unchecked")
    public T get(Hash hash) {
        String key = hash.encode().substring(0, keyLength);
        int index = Integer.parseInt(key, 16);
        return (T) buckets[index];
    }

    private static String keyOf(int index, int keyLength) {
        String hex = Integer.toHexString(index);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < keyLength - hex.length(); i++) {
            builder.append('0');
        }
        return builder.append(hex).toString();
    }

    private static int size(int keyLength) {
        return 1 << (4 * keyLength);
    }

    public static Set<String> keySet(int keyLength) {
        int size = size(keyLength);
        Set<String> keySet = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            keySet.add(keyOf(i, keyLength));
        }
        return keySet;
    }
}
