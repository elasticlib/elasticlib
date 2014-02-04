package store.common.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.value.Value;

/**
 * Unit tests.
 */
public class ObjectEncoderTest {

    /**
     * Test.
     */
    @Test
    public void encodeNull() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x06, // length
                                0x01, // type
                                0x6E, 0x75, 0x6C, 0x6C, 0x00); // key (no value)
        byte[] actual = new ObjectEncoder()
                .putNull("null")
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeBoolean() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x07, // length
                                0x03, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x01); // value
        byte[] actual = new ObjectEncoder()
                .put("bool", true)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeLong() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0C, // length
                                0x06, // type
                                0x6C, 0x67, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = new ObjectEncoder()
                .put("lg", 1432897L)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeMap() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x26, // length
                                0x0A, // type
                                0x6D, 0x61, 0x70, 0x00, // key
                                // value
                                0x00, 0x00, 0x00, 0x1D, // length
                                0x05, // type
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, // entry
                                0x08, // type
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                0x03, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x00); // entry

        Map<String, Value> map = new LinkedHashMap<>();
        map.put("num", Value.of(4));
        map.put("str", Value.of("test"));
        map.put("bool", Value.of(false));

        byte[] actual = new ObjectEncoder()
                .put("map", map)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeEmptyMap() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x09, // length
                                0x0A, // type
                                0x6D, 0x61, 0x70, 0x00, // key
                                0x00, 0x00, 0x00, 0x00); // value (length only)

        byte[] actual = new ObjectEncoder()
                .put("map", Collections.<String, Value>emptyMap())
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeList() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x18, // length
                                0x0B, // type
                                0x6C, 0x69, 0x73, 0x74, 0x00, // key
                                // value
                                0x00, 0x00, 0x00, 0x0E, // length
                                0x05, // type
                                0x00, 0x00, 0x00, 0x0A, // entry
                                0x08, // type
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // entry

        List<Value> list = new ArrayList<>();
        list.add(Value.of(10));
        list.add(Value.of("test"));

        byte[] actual = new ObjectEncoder()
                .put("list", list)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeEmptyList() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0A, // length
                                0x0B, // type
                                0x6C, 0x69, 0x73, 0x74, 0x00, // key
                                0x00, 0x00, 0x00, 0x00); // value (length only)

        byte[] actual = new ObjectEncoder()
                .put("list", Collections.<Value>emptyList())
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encode() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x1D, // length
                                0x05, // type
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, // entry
                                0x08, // type
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                0x03, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x00); // entry

        byte[] actual = new ObjectEncoder()
                .put("num", 4)
                .put("str", "test")
                .put("bool", false)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeValue() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x1D, // length
                                0x05, // type
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, // entry
                                0x08, // type
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                0x03, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x00); // entry

        byte[] actual = new ObjectEncoder()
                .put("num", Value.of(4))
                .put("str", Value.of("test"))
                .put("bool", Value.of(false))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    private static byte[] array(int... values) {
        byte[] array = new byte[values.length];
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) values[i];
        }
        return array;
    }
}
