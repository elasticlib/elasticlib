package store.server;

import store.common.Hash;

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
}
