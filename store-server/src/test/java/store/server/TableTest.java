package store.server;

import store.server.lock.Table;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.Hash;

public class TableTest {

    private static final char[] ALPHABET = new char[]{'0', '1', '2', '3',
                                                      '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b',
                                                      'c', 'd', 'e', 'f'};

    @Test
    public void createAndGetTest() {
        Table table = new Table<String>(2) {
            @Override
            protected String initialValue(String key) {
                return key;
            }
        };

        for (char c1 : ALPHABET) {
            for (char c2 : ALPHABET) {
                String key = "" + c1 + c2;
                Hash hash = new Hash(key + "00110011001100110011001100110011001100");
                assertThat(table.get(hash)).isEqualTo(key);
            }
        }
    }
}
