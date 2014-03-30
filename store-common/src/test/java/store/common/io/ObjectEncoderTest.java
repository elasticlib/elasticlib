package store.common.io;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestUtil.array;
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
    public void encodeByteArray() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0D, // length
                                0x02, // type
                                0x62, 0x79, 0x74, 0x65, 0x73, 0x00, // key
                                0x00, 0x00, 0x00, 0x02,
                                0x12, 0x34); // value

        byte[] actual = new ObjectEncoder()
                .put("bytes", array(0x12, 0x34))
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
    public void encodeByte() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x07, // length
                                0x04, // type
                                0x62, 0x79, 0x74, 0x65, 0x00, // key
                                0xFF); // value
        byte[] actual = new ObjectEncoder()
                .put("byte", (byte) 0xFF)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeInt() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x09, // length
                                0x05, // type
                                0x69, 0x6E, 0x74, 0x00, // key
                                0x00, 0x01, 0xE2, 0x40); // value
        byte[] actual = new ObjectEncoder()
                .put("int", 123456)
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
    public void encodeBigDecimal() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x10, // length
                                0x07, // type
                                0x64, 0x65, 0x63, 0x69, 0x6D, 0x61, 0x6C, 0x00, // key
                                0x00, 0x00, 0x00, 0x03, 0x31, 0x2E, 0x31); // value
        byte[] actual = new ObjectEncoder()
                .put("decimal", new BigDecimal("1.1"))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeString() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x10, // length
                                0x08, // type
                                0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // value
        byte[] actual = new ObjectEncoder()
                .put("string", "test")
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void encodeDate() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0E, // length
                                0x09, // type
                                0x64, 0x61, 0x74, 0x65, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = new ObjectEncoder()
                .put("date", new Date(1432897L))
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

        Map<String, Value> map = new LinkedHashMap<>();
        map.put("num", Value.of(4));
        map.put("str", Value.of("test"));
        map.put("bool", Value.of(false));

        byte[] actual = new ObjectEncoder()
                .put(map)
                .build();

        assertThat(actual).isEqualTo(expected);
    }
}
