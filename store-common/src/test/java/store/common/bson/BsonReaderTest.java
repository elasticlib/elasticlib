package store.common.bson;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.fest.assertions.api.Assertions.assertThat;
import org.joda.time.Instant;
import org.testng.annotations.Test;
import static store.common.TestUtil.array;
import static store.common.bson.BsonType.*;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Unit tests.
 */
public class BsonReaderTest {

    /**
     * Test.
     */
    @Test
    public void containsKey() {
        byte[] bytes = array(BOOLEAN,
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x01); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.containsKey("bool")).isTrue();
        assertThat(reader.containsKey("unknown")).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void keySet() {
        byte[] bytes = array(INTEGER,
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // entry
                             STRING,
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             BOOLEAN,
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x00); // entry

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.keyset()).containsExactly("num", "str", "bool");
    }

    /**
     * Test.
     */
    @Test
    public void asMap() {
        byte[] bytes = array(INTEGER,
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // entry
                             STRING,
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             BOOLEAN,
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x00); // entry

        Map<String, Value> map = new HashMap<>();
        map.put("num", Value.of(4));
        map.put("str", Value.of("test"));
        map.put("bool", Value.of(false));

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.asMap()).isEqualTo(map);
    }

    /**
     * Test.
     */
    @Test
    public void readNull() {
        byte[] bytes = array(NULL,
                             0x6E, 0x75, 0x6C, 0x6C, 0x00); // key (no value)

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.get("null").type()).isEqualTo(ValueType.NULL);
    }

    /**
     * Test.
     */
    @Test
    public void readHash() {
        byte[] bytes = array(HASH,
                             0x68, 0x61, 0x73, 0x68, 0x00, // key
                             0x8D, 0x5F, 0x3C, 0x77, 0xE9, 0x4A, 0x0C, 0xAD, 0x3A, 0x32,
                             0x34, 0x0D, 0x34, 0x21, 0x35, 0xF4, 0x3D, 0xBB, 0x7C, 0xBB); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getHash("hash")).isEqualTo(new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"));
    }

    /**
     * Test.
     */
    @Test
    public void readGuid() {
        byte[] bytes = array(GUID,
                             0x67, 0x75, 0x69, 0x64, 0x00, // key
                             0x8D, 0x5F, 0x3C, 0x77, 0xE9, 0x4A, 0x0C, 0xAD,
                             0x3A, 0x32, 0x34, 0x0D, 0x34, 0x21, 0x35, 0xF4); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getGuid("guid")).isEqualTo(new Guid("8d5f3c77e94a0cad3a32340d342135f4"));
    }

    /**
     * Test.
     */
    @Test
    public void readByteArray() {
        byte[] bytes = array(BINARY,
                             0x62, 0x79, 0x74, 0x65, 0x73, 0x00, // key
                             0x00, 0x00, 0x00, 0x02,
                             0x12, 0x34); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getByteArray("bytes")).isEqualTo(array(0x12, 0x34));
    }

    /**
     * Test.
     */
    @Test
    public void readBoolean() {
        byte[] bytes = array(BOOLEAN,
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x01); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getBoolean("bool")).isEqualTo(true);
    }

    /**
     * Test.
     */
    @Test
    public void readLong() {
        byte[] bytes = array(INTEGER,
                             0x6C, 0x67, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getLong("lg")).isEqualTo(1432897);
    }

    /**
     * Test.
     */
    @Test
    public void readBigDecimal() {
        byte[] bytes = array(DECIMAL,
                             0x64, 0x65, 0x63, 0x69, 0x6D, 0x61, 0x6C, 0x00, // key
                             0x00, 0x00, 0x00, 0x03, 0x31, 0x2E, 0x31); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getBigDecimal("decimal")).isEqualTo(new BigDecimal("1.1"));
    }

    /**
     * Test.
     */
    @Test
    public void readString() {
        byte[] bytes = array(STRING,
                             0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getString("string")).isEqualTo("test");
    }

    /**
     * Test.
     */
    @Test
    public void readDate() {
        byte[] bytes = array(DATE,
                             0x64, 0x61, 0x74, 0x65, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0xDD, 0x41); // value

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getInstant("date")).isEqualTo(new Instant(1432897));
    }

    /**
     * Test.
     */
    @Test
    public void readMap() {
        byte[] bytes = array(OBJECT,
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

        Map<String, Value> map = new HashMap<>();
        map.put("num", Value.of(4));
        map.put("str", Value.of("test"));
        map.put("bool", Value.of(false));

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getMap("map")).isEqualTo(map);
    }

    /**
     * Test.
     */
    @Test
    public void readEmptyMap() {
        byte[] bytes = array(OBJECT,
                             0x6D, 0x61, 0x70, 0x00, // key
                             0x00, 0x00, 0x00, 0x00); // value (length only)

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getMap("map")).isEmpty();
    }

    /**
     * Test.
     */
    @Test
    public void readList() {
        byte[] bytes = array(ARRAY,
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

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getList("list")).isEqualTo(list);
    }

    /**
     * Test.
     */
    @Test
    public void readEmptyList() {
        byte[] bytes = array(ARRAY,
                             0x6C, 0x69, 0x73, 0x74, 0x00, // key
                             0x00, 0x00, 0x00, 0x00); // value (length only)

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getList("list")).isEmpty();
    }

    /**
     * Test.
     */
    @Test
    public void read() {
        byte[] bytes = array(INTEGER,
                             0x6E, 0x75, 0x6D, 0x00, // key
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // entry
                             STRING,
                             0x73, 0x74, 0x72, 0x00, // key
                             0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // entry
                             BOOLEAN,
                             0x62, 0x6F, 0x6F, 0x6C, 0x00, // key
                             0x00); // entry

        BsonReader reader = new BsonReader(bytes);
        assertThat(reader.getLong("num")).isEqualTo(4);
        assertThat(reader.getString("str")).isEqualTo("test");
        assertThat(reader.getBoolean("bool")).isEqualTo(false);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = NoSuchElementException.class)
    public void getUnknownKey() {
        new BsonReader(new byte[0]).get("unknown");
    }
}
