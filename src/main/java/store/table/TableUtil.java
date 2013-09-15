package store.table;

final class TableUtil {

    private static final char[] alphabet = new char[]{'0', '1', '2', '3',
                                                      '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b',
                                                      'c', 'd', 'e', 'f'};

    private TableUtil() {
        // Non-instanciable.
    }

    public static int indexOf(String key) {
        int index = 0;
        for (int i = 0; i < key.length(); i++) {
            index += pow(key.length() - i - 1) * position(key.charAt(i));
        }
        return index;
    }

    private static int pow(int i) {
        return 1 << (4 * i);
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
