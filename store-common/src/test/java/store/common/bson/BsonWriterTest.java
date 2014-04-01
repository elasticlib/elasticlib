package store.common.bson;

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
public class BsonWriterTest {

    /**
     * Test.
     */
    @Test
    public void writeNull() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x06, // length
                                0x01, // type
                                0x6E, 0x75, 0x6C, 0x6C, 0x00); // key (no value)
        byte[] actual = new BsonWriter()
                .putNull("null")
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeByteArray() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0D, // length
                                0x02, // type
                                0x62, 0x79, 0x74, 0x65, 0x73, 0x00, // key
                                0x00, 0x00, 0x00, 0x02,
                                0x12, 0x34); // value

        byte[] actual = new BsonWriter()
                .put("bytes", array(0x12, 0x34))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeBoolean() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x07, // length
                                0x03, // type
                                0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                                0x01); // value
        byte[] actual = new BsonWriter()
                .put("bool", true)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeByte() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x07, // length
                                0x04, // type
                                0x62, 0x79, 0x74, 0x65, 0x00, // key
                                0xFF); // value
        byte[] actual = new BsonWriter()
                .put("byte", (byte) 0xFF)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeInt() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x09, // length
                                0x05, // type
                                0x69, 0x6E, 0x74, 0x00, // key
                                0x00, 0x01, 0xE2, 0x40); // value
        byte[] actual = new BsonWriter()
                .put("int", 123456)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeLong() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0C, // length
                                0x06, // type
                                0x6C, 0x67, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = new BsonWriter()
                .put("lg", 1432897L)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeBigDecimal() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x10, // length
                                0x07, // type
                                0x64, 0x65, 0x63, 0x69, 0x6D, 0x61, 0x6C, 0x00, // key
                                0x00, 0x00, 0x00, 0x03, 0x31, 0x2E, 0x31); // value
        byte[] actual = new BsonWriter()
                .put("decimal", new BigDecimal("1.1"))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeString() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x10, // length
                                0x08, // type
                                0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // value
        byte[] actual = new BsonWriter()
                .put("string", "test")
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeDate() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0E, // length
                                0x09, // type
                                0x64, 0x61, 0x74, 0x65, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = new BsonWriter()
                .put("date", new Date(1432897L))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeMap() {
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

        byte[] actual = new BsonWriter()
                .put("map", map)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeEmptyMap() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x09, // length
                                0x0A, // type
                                0x6D, 0x61, 0x70, 0x00, // key
                                0x00, 0x00, 0x00, 0x00); // value (length only)

        byte[] actual = new BsonWriter()
                .put("map", Collections.<String, Value>emptyMap())
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeList() {
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

        byte[] actual = new BsonWriter()
                .put("list", list)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeEmptyList() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0A, // length
                                0x0B, // type
                                0x6C, 0x69, 0x73, 0x74, 0x00, // key
                                0x00, 0x00, 0x00, 0x00); // value (length only)

        byte[] actual = new BsonWriter()
                .put("list", Collections.<Value>emptyList())
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeValue() {
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

        byte[] actual = new BsonWriter()
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
    public void write() {
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

        byte[] actual = new BsonWriter()
                .put(map)
                .build();

        assertThat(actual).isEqualTo(expected);
    }
}
