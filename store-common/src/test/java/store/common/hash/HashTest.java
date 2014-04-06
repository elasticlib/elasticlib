package store.common.hash;

import java.util.Set;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestUtil.array;

/**
 * Units tests.
 */
public class HashTest {

    private static final byte[] BYTES = array(0x8d, 0x5f, 0x3c, 0x77, 0xe9, 0x4a, 0x0c, 0xad, 0x3a, 0x32);
    private static final String HEXADECIMAL = "8d5f3c77e94a0cad3a32";
    private static final String LESS = "2827c43f0aad546501f9";
    private static final String MORE = "9c42e74cae7674273aba";
    private static final char[] ALPHABET = new char[]{'0', '1', '2', '3',
                                                      '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b',
                                                      'c', 'd', 'e', 'f'};

    /**
     * Test.
     */
    @Test
    public void asHexadecimalStringTest() {
        assertThat(new Hash(BYTES).asHexadecimalString()).isEqualTo(HEXADECIMAL);
    }

    /**
     * Test.
     */
    @Test
    public void getBytesTest() {
        assertThat(new Hash(HEXADECIMAL).getBytes()).isEqualTo(BYTES);
    }

    /**
     * Test.
     */
    @Test
    public void keyTest() {
        Hash hash = new Hash(BYTES);
        assertThat(hash.key(0)).isEmpty();
        assertThat(hash.key(1)).isEqualTo("8");
        assertThat(hash.key(2)).isEqualTo("8d");
        assertThat(hash.key(3)).isEqualTo("8d5");
        assertThat(hash.key(4)).isEqualTo("8d5f");
    }

    /**
     * Test.
     */
    @Test
    public void keySetTest() {
        Set<String> keySet = Hash.keySet(2);
        assertThat(keySet).hasSize(256);
        for (char c1 : ALPHABET) {
            for (char c2 : ALPHABET) {
                String key = "" + c1 + c2;
                assertThat(keySet.contains(key));
            }
        }
    }

    /**
     * Test.
     */
    @Test
    public void emptyKeySetTest() {
        assertThat(Hash.keySet(0)).isEmpty();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void keySetWithNegativeLengthTest() {
        Hash.keySet(-1);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void keySetWithExcessiveLengthTest() {
        Hash.keySet(6);
    }

    /**
     * Test.
     */
    @Test
    public void hashCodeTest() {
        int actual = new Hash(HEXADECIMAL).hashCode();
        int same = new Hash(HEXADECIMAL).hashCode();
        int other = new Hash(LESS).hashCode();
        assertThat(actual).isEqualTo(same);
        assertThat(actual).isNotEqualTo(other);
    }

    /**
     * Test.
     */
    @Test
    @SuppressWarnings({"ObjectEqualsNull", "IncompatibleEquals"})
    public void equalsTest() {
        Hash hash = new Hash(HEXADECIMAL);
        Hash same = new Hash(HEXADECIMAL);
        Hash other = new Hash(LESS);

        assertThat(hash.equals(same)).isTrue();
        assertThat(hash.equals(other)).isFalse();
        assertThat(hash.equals(null)).isFalse();
        assertThat(hash.equals("should not match")).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void compareToTest() {
        Hash hash = new Hash(HEXADECIMAL);
        Hash same = new Hash(HEXADECIMAL);
        Hash less = new Hash(LESS);
        Hash more = new Hash(MORE);

        assertThat(hash.compareTo(same)).isZero();
        assertThat(hash.compareTo(less)).isPositive();
        assertThat(hash.compareTo(more)).isNegative();
    }
}
