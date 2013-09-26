package store.server;

import store.common.Hash;

public abstract class Table<T> {

    private static final char[] ALPHABET = new char[]{'0', '1', '2', '3',
                                                      '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b',
                                                      'c', 'd', 'e', 'f'};
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

    private static int indexOf(String key) {
        int index = 0;
        for (int i = 0; i < key.length(); i++) {
            index += pow(key.length() - i - 1) * position(key.charAt(i));
        }
        return index;
    }

    private static String keyOf(int index, int keyLength) {
        String hex = Integer.toHexString(index);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < keyLength - hex.length(); i++) {
            builder.append('0');
        }
        return builder.append(hex).toString();
    }

    private static int pow(int i) {
        return 1 << (4 * i);
    }

    private static int position(char c) {
        for (int i = 0; i < ALPHABET.length; i++) {
            if (ALPHABET[i] == c) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.valueOf(c));
    }
}
