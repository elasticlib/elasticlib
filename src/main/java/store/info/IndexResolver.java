package store.info;

import store.hash.Hash;

final class IndexResolver {

    private static final char[] alphabet = new char[]{'0', '1', '2', '3',
                                                      '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b',
                                                      'c', 'd', 'e', 'f'};

    private IndexResolver() {
        // Non-instanciable.
    }

    public static int index(Hash hash) {
        int index = 0;
        String key = hash.encode()
                .substring(0, 2);

        for (int i = 0; i < key.length(); i++) {
            index += pow(key.length() - i - 1) * position(key.charAt(i));
        }
        return index;

    }

    private static int pow(int i) {
        int pow = 1;
        for (int j = 0; j < i; j++) {
            pow *= alphabet.length;
        }
        return pow;
    }

    private static int position(char c) {
        for (int i = 0; i < alphabet.length; i++) {
            if (alphabet[i] == c) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.valueOf(c));
    }
}
