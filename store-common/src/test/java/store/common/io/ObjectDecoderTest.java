package store.common.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class ObjectDecoderTest {

    /**
     * Test.
     */
    @Test
    public void containsKey() {
        byte[] bytes = array(0x02, // type
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x01); // value

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.containsKey("bool")).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void decodeBoolean() {
        byte[] bytes = array(0x02, // type
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
        byte[] bytes = array(0x05, // type
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
        byte[] bytes = array(0x08, // type
                             0x6D, 0x61, 0x70, 0x00, // key
                             // value
                             0x00, 0x00, 0x00, 0x1D, // length
                             0x04, // type
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, // entry
                             0x06, // type
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             0x02, // type
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x00); // entry

        Map<String, Object> map = new HashMap<>();
        map.put("num", 4);
        map.put("str", "test");
        map.put("bool", false);

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getMap("map")).isEqualTo(map);
    }

    /**
     * Test.
     */
    @Test
    public void decodeEmptyMap() {
        byte[] bytes = array(0x08, // type
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
        byte[] bytes = array(0x09, // type
                             0x6C, 0x69, 0x73, 0x74, 0x00, // key
                             // value
                             0x00, 0x00, 0x00, 0x0E, // length
                             0x04, // type
                             0x00, 0x00, 0x00, 0x0A, // entry
                             0x06, // type
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // entry

        List<Object> list = new ArrayList<>();
        list.add(10);
        list.add("test");

        ObjectDecoder decoder = new ObjectDecoder(bytes);
        assertThat(decoder.getList("list")).isEqualTo(list);
    }

    /**
     * Test.
     */
    @Test
    public void decodeEmptyList() {
        byte[] bytes = array(0x09, // type
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
        byte[] bytes = array(0x04, // type
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, // entry
                             0x06, // type
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             0x02, // type
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