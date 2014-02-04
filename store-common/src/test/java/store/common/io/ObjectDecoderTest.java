package store.common.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static byte[] array(int... values) {
        byte[] array = new byte[values.length];
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) values[i];
        }
        return array;
    }
}
