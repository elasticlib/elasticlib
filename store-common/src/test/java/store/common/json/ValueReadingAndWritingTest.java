package store.common.json;

import java.math.BigDecimal;
import static java.util.Arrays.asList;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonValue;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.common.Hash;
import static store.common.json.ValueReading.readValue;
import static store.common.json.ValueWriting.writeValue;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.STRING;

/**
 * Unit tests.
 */
public class ValueReadingAndWritingTest {

    private static final Map<ValueType, Value> VALUES = new EnumMap<>(ValueType.class);
    private static final Map<ValueType, JsonValue> JSON = new EnumMap<>(ValueType.class);

    static {
        put(Value.ofNull(),
            createObjectBuilder()
                .add("type", "NULL")
                .addNull("value")
                .build());

        put(Value.of((byte) 0x1a),
            createObjectBuilder()
                .add("type", "BYTE")
                .add("value", "1a")
                .build());

        put(Value.of(new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb").value()),
            createObjectBuilder()
                .add("type", "BYTE_ARRAY")
                .add("value", "8d5f3c77e94a0cad3a32340d342135f43dbb7cbb")
                .build());

        put(Value.of(true),
            createObjectBuilder()
                .add("type", "BOOLEAN")
                .add("value", true)
                .build());

        put(Value.of(10),
            createObjectBuilder()
                .add("type", "INTEGER")
                .add("value", 10)
                .build());

        put(Value.of(120L),
            createObjectBuilder()
                .add("type", "LONG")
                .add("value", 120L)
                .build());

        put(Value.of(new BigDecimal("3.14159265359")),
            createObjectBuilder()
                .add("type", "BIG_DECIMAL")
                .add("value", "3.14159265359")
                .build());

        put(Value.of("test"),
            createObjectBuilder()
                .add("type", "STRING")
                .add("value", "test")
                .build());

        put(Value.of(new Date(1391878872)),
            createObjectBuilder()
                .add("type", "DATE")
                .add("value", 1391878872)
                .build());

        Map<String, Value> map = new LinkedHashMap<>();
        map.put("num", VALUES.get(INTEGER));
        map.put("text", VALUES.get(STRING));
        map.put("bool", VALUES.get(BOOLEAN));
        put(Value.of(map),
            createObjectBuilder()
                .add("type", "MAP")
                .add("value",
                     createObjectBuilder()
                .add("num", JSON.get(INTEGER))
                .add("text", JSON.get(STRING))
                .add("bool", JSON.get(BOOLEAN)))
                .build());

        put(Value.of(asList(VALUES.get(INTEGER),
                            VALUES.get(STRING),
                            VALUES.get(BOOLEAN))),
            createObjectBuilder()
                .add("type", "LIST")
                .add("value",
                     createArrayBuilder()
                .add(JSON.get(INTEGER))
                .add(JSON.get(STRING))
                .add(JSON.get(BOOLEAN)))
                .build());
    }

    private static void put(Value value, JsonValue json) {
        ValueType type = value.type();
        VALUES.put(type, value);
        JSON.put(type, json);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "readValueDataProvider")
    public Object[][] readValueDataProvider() {
        ValueType[] types = ValueType.values();
        Object[][] data = new Object[types.length][];
        for (int i = 0; i < types.length; i++) {
            data[i] = new Object[]{JSON.get(types[i]), VALUES.get(types[i])};
        }
        return data;
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "writeValueDataProvider")
    public Object[][] writeValueDataProvider() {
        ValueType[] types = ValueType.values();
        Object[][] data = new Object[types.length][];
        for (int i = 0; i < types.length; i++) {
            data[i] = new Object[]{VALUES.get(types[i]), JSON.get(types[i])};
        }
        return data;
    }

    /**
     * Test.
     *
     * @param json Input data.
     * @param expected Expected Output data.
     */
    @Test(dataProvider = "readValueDataProvider")
    public void readValueTest(JsonValue json, Value expected) {
        assertThat(readValue(json))
                .as(expected.type().toString())
                .isEqualTo(expected);
    }

    /**
     * Test.
     *
     * @param value Input data.
     * @param expected Expected Output data.
     */
    @Test(dataProvider = "writeValueDataProvider")
    public void writeValueTest(Value value, JsonValue expected) {
        assertThat(writeValue(value))
                .as(value.type().toString())
                .isEqualTo(expected);
    }
}
