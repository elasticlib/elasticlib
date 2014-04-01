package store.common.bson;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestUtil.array;
import static store.common.bson.BsonType.*;
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
        byte[] bytes = array(LONG,
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
        byte[] bytes = array(LONG,
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
        byte[] bytes = array(LONG,
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
        byte[] bytes = array(BIG_DECIMAL,
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
        assertThat(reader.getDate("date")).isEqualTo(new Date(1432897));
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
                             LONG,
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
                             LONG,
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
        byte[] bytes = array(LONG,
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
