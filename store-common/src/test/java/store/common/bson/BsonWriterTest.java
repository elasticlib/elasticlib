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
import static store.common.bson.BsonType.ARRAY;
import static store.common.bson.BsonType.BINARY;
import static store.common.bson.BsonType.BOOLEAN;
import static store.common.bson.BsonType.DATE;
import static store.common.bson.BsonType.DECIMAL;
import static store.common.bson.BsonType.GUID;
import static store.common.bson.BsonType.HASH;
import static store.common.bson.BsonType.INTEGER;
import static store.common.bson.BsonType.NULL;
import static store.common.bson.BsonType.OBJECT;
import static store.common.bson.BsonType.STRING;
import store.common.hash.Guid;
import store.common.hash.Hash;
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
        byte[] expected = array(NULL,
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
    public void writeHash() {
        byte[] expected = array(HASH,
                                0x68, 0x61, 0x73, 0x68, 0x00, // key
                                0x8D, 0x5F, 0x3C, 0x77, 0xE9, 0x4A, 0x0C, 0xAD, 0x3A, 0x32,
                                0x34, 0x0D, 0x34, 0x21, 0x35, 0xF4, 0x3D, 0xBB, 0x7C, 0xBB); // value

        byte[] actual = new BsonWriter()
                .put("hash", new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeGuid() {
        byte[] expected = array(GUID,
                                0x67, 0x75, 0x69, 0x64, 0x00, // key
                                0x8D, 0x5F, 0x3C, 0x77, 0xE9, 0x4A, 0x0C, 0xAD,
                                0x3A, 0x32, 0x34, 0x0D, 0x34, 0x21, 0x35, 0xF4); // value

        byte[] actual = new BsonWriter()
                .put("guid", new Guid("8d5f3c77e94a0cad3a32340d342135f4"))
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void writeByteArray() {
        byte[] expected = array(BINARY,
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
        byte[] expected = array(BOOLEAN,
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
        byte[] expected = array(INTEGER,
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
        byte[] expected = array(DECIMAL,
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
        byte[] expected = array(STRING,
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
        byte[] expected = array(DATE,
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
        byte[] expected = array(OBJECT,
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
        byte[] expected = array(OBJECT,
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
        byte[] expected = array(ARRAY,
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
        byte[] expected = array(ARRAY,
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
        byte[] expected = array(INTEGER,
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
        byte[] expected = array(INTEGER,
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
