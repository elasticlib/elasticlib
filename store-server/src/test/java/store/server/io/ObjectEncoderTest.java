package store.server.io;

import java.util.ArrayList;
import java.util.Collections;
import static java.util.Collections.emptyList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.server.io.ObjectEncoder.encoder;

public class ObjectEncoderTest {

    @Test
    public void encodeBooleanTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x07, // length
                                0x02, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x01); // value
        byte[] actual = encoder()
                .put("bool", true)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void encodeLongTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0C, // length
                                0x04, // type
                                0x6C, 0x67, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = encoder()
                .put("lg", 1432897L)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void encodeMapTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x26, // length
                                0x07, // type
                                0x6D, 0x61, 0x70, 0x00, // key
                                // value
                                0x00, 0x00, 0x00, 0x1D, // length
                                0x03, // type
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, // entry
                                0x05, // type
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                0x02, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x00); // entry

        Map<String, Object> map = new HashMap<>();
        map.put("num", 4);
        map.put("str", "test");
        map.put("bool", false);

        byte[] actual = encoder()
                .put("map", map)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void encodeEmptyMapTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x09, // length
                                0x07, // type
                                0x6D, 0x61, 0x70, 0x00, // key
                                0x00, 0x00, 0x00, 0x00); // value (length only)

        byte[] actual = encoder()
                .put("map", Collections.<String, Object>emptyMap())
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void encodeListTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x18, // length
                                0x08, // type
                                0x6C, 0x69, 0x73, 0x74, 0x00, // key
                                // value
                                0x00, 0x00, 0x00, 0x0E, // length
                                0x03, // type
                                0x00, 0x00, 0x00, 0x0A, // entry
                                0x05, // type
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // entry

        List<Object> list = new ArrayList<>();
        list.add(10);
        list.add("test");

        byte[] actual = encoder()
                .put("list", list)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void encodeEmptyListTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0A, // length
                                0x08, // type
                                0x6C, 0x69, 0x73, 0x74, 0x00, // key
                                0x00, 0x00, 0x00, 0x00); // value (length only)

        byte[] actual = encoder()
                .put("list", emptyList())
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void encodeTest() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x1D, // length
                                0x03, // type
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, // entry
                                0x05, // type
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                0x02, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x00); // entry

        byte[] actual = encoder()
                .put("num", 4)
                .put("str", "test")
                .put("bool", false)
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
