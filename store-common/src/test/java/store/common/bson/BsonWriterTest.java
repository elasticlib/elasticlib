package store.common.bson;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.joda.time.Instant;
import org.testng.annotations.Test;
import static store.common.TestUtil.array;
import static store.common.bson.BsonType.*;
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
                                NULL,
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
                                BINARY,
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
                                BOOLEAN,
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
    public void writeLong() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x0C, // length
                                INTEGER,
                                0x6C, 0x67, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = new BsonWriter()
                .put("lg", 1432897)
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeBigDecimal() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x10, // length
                                DECIMAL,
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
                                STRING,
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
                                DATE,
                                0x64, 0x61, 0x74, 0x65, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value
        byte[] actual = new BsonWriter()
                .put("date", new Instant(1432897))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeMap() {
        byte[] expected = array(0x00, 0x00, 0x00, 0x2A, // length
                                OBJECT,
                                0x6D, 0x61, 0x70, 0x00, // key
                                // value
                                0x00, 0x00, 0x00, 0x21, // length
                                INTEGER,
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // entry
                                STRING,
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                BOOLEAN,
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
                                OBJECT,
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
        byte[] expected = array(0x00, 0x00, 0x00, 0x1C, // length
                                ARRAY,
                                0x6C, 0x69, 0x73, 0x74, 0x00, // key
                                // value
                                0x00, 0x00, 0x00, 0x12, // length
                                INTEGER,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, // entry
                                STRING,
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
                                ARRAY,
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
        byte[] expected = array(0x00, 0x00, 0x00, 0x21, // length
                                INTEGER,
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // entry
                                STRING,
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                BOOLEAN,
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
        byte[] expected = array(0x00, 0x00, 0x00, 0x21, // length
                                INTEGER,
                                0x6E, 0x75, 0x6D, 0x00, // key
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // entry
                                STRING,
                                0x73, 0x74, 0x72, 0x00, // key
                                0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                                BOOLEAN,
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
