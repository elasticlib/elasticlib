package store.common.io;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Unit tests.
 */
public class ObjectDecoderTest {

    /**
     * Test.
     */
    @Test
    public void containsKey() {
        byte[] bytes = array(0x03, // type
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x01); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.containsKey("bool")).isTrue();
        assertThat(decoder.containsKey("unknown")).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void keySet() {
        byte[] bytes = array(0x05, // type
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, // entry
                             0x08, // type
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             0x03, // type
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x00); // entry

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.keyset()).containsExactly("num", "str", "bool");
    }

    /**
     * Test.
     */
    @Test
    public void decodeNull() {
        byte[] bytes = array(0x01, // type
                             0x6E, 0x75, 0x6C, 0x6C, 0x00); // key (no value)

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.get("null").type()).isEqualTo(ValueType.NULL);
    }

    /**
     * Test.
     */
    @Test
    public void decodeByteArray() {
        byte[] bytes = array(0x02, // type
                             0x62, 0x79, 0x74, 0x65, 0x73, 0x00, // key
                             0x00, 0x00, 0x00, 0x02,
                             0x12, 0x34); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getByteArray("bytes")).isEqualTo(array(0x12, 0x34));
    }

    /**
     * Test.
     */
    @Test
    public void decodeBoolean() {
        byte[] bytes = array(0x03, // type
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x01); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getBoolean("bool")).isEqualTo(true);
    }

    /**
     * Test.
     */
    @Test
    public void decodeByte() {
        byte[] bytes = array(0x04, // type
                             0x62, 0x79, 0x74, 0x65, 0x00, // key
                             0xFF); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getByte("byte")).isEqualTo((byte) 0xFF);
    }

    /**
     * Test.
     */
    @Test
    public void decodeInt() {
        byte[] bytes = array(0x05, // type
                             0x69, 0x6E, 0x74, 0x00, // key
                             0x00, 0x01, 0xE2, 0x40); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getInt("int")).isEqualTo(123456);
    }

    /**
     * Test.
     */
    @Test
    public void decodeLong() {
        byte[] bytes = array(0x06, // type
                             0x6C, 0x67, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getLong("lg")).isEqualTo(1432897L);
    }

    /**
     * Test.
     */
    @Test
    public void decodeBigDecimal() {
        byte[] bytes = array(0x07, // type
                             0x64, 0x65, 0x63, 0x69, 0x6D, 0x61, 0x6C, 0x00, // key
                             0x00, 0x00, 0x00, 0x03, 0x31, 0x2E, 0x31); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getBigDecimal("decimal")).isEqualTo(new BigDecimal("1.1"));
    }

    /**
     * Test.
     */
    @Test
    public void decodeString() {
        byte[] bytes = array(0x08, // type
                             0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getString("string")).isEqualTo("test");
    }

    /**
     * Test.
     */
    @Test
    public void decodeDate() {
        byte[] bytes = array(0x09, // type
                             0x64, 0x61, 0x74, 0x65, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getDate("date")).isEqualTo(new Date(1432897L));
    }

    /**
     * Test.
     */
    @Test
    public void decodeMap() {
        byte[] bytes = array(0x0A, // type
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

        Map<String, Value> map = new HashMap<>();
        map.put("num", Value.of(4));
        map.put("str", Value.of("test"));
        map.put("bool", Value.of(false));

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getMap("map")).isEqualTo(map);
    }

    /**
     * Test.
     */
    @Test
    public void decodeEmptyMap() {
        byte[] bytes = array(0x0A, // type
                             0x6D, 0x61, 0x70, 0x00, // key
                             0x00, 0x00, 0x00, 0x00); // value (length only)

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getMap("map")).isEmpty();
    }

    /**
     * Test.
     */
    @Test
    public void decodeList() {
        byte[] bytes = array(0x0B, // type
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

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getList("list")).isEqualTo(list);
    }

    /**
     * Test.
     */
    @Test
    public void decodeEmptyList() {
        byte[] bytes = array(0x0B, // type
                             0x6C, 0x69, 0x73, 0x74, 0x00, // key
                             0x00, 0x00, 0x00, 0x00); // value (length only)

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getList("list")).isEmpty();
    }

    /**
     * Test.
     */
    @Test
    public void decode() {
        byte[] bytes = array(0x05, // type
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, // entry
                             0x08, // type
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             0x03, // type
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x00); // entry

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getInt("num")).isEqualTo(4);
        assertThat(decoder.getString("str")).isEqualTo("test");
        assertThat(decoder.getBoolean("bool")).isEqualTo(false);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = NoSuchElementException.class)
    public void getUnknownKey() {
        new ObjectDecoder(new byte[0]).get("unknown");
    }

    private static byte[] array(int... values) {
        byte[] array = new byte[values.length];
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) values[i];
        }
        return array;
    }
}
