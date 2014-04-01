package store.common.json;

import com.google.common.collect.ImmutableMap;
import static com.google.common.io.BaseEncoding.base16;
import java.math.BigDecimal;
import static java.util.Arrays.asList;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonValue;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static store.common.json.ValueReading.readValue;
import static store.common.json.ValueWriting.writeValue;
import store.common.json.schema.Schema;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.STRING;

/**
 * Unit tests.
 */
public class ValueReadingAndWritingTest {

    private static final Map<ValueType, Value> VALUES = new EnumMap<>(ValueType.class);
    private static final Map<ValueType, JsonValue> JSON = new EnumMap<>(ValueType.class);

    static {
        put(Value.ofNull(),
            JsonValue.NULL);

        put(Value.of(base16().lowerCase().decode("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb")),
            jsonString("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"));


        put(Value.of(true),
            JsonValue.TRUE);

        put(Value.of(120),
            jsonNumber(120));

        put(Value.of(new BigDecimal("3.14159265359")),
            jsonNumber(new BigDecimal("3.14159265359")));

        put(Value.of("test"),
            jsonString("test"));

        put(Value.of(new Date(1391878872)),
            jsonNumber(1391878872));

        put(Value.of(ImmutableMap.of("num", VALUES.get(LONG),
                                     "text", VALUES.get(STRING),
                                     "bool", VALUES.get(BOOLEAN))),
            createObjectBuilder()
                .add("num", JSON.get(LONG))
                .add("text", JSON.get(STRING))
                .add("bool", JSON.get(BOOLEAN))
                .build());

        put(Value.of(asList(VALUES.get(LONG),
                            VALUES.get(STRING),
                            VALUES.get(BOOLEAN))),
            createArrayBuilder()
                .add(JSON.get(LONG))
                .add(JSON.get(STRING))
                .add(JSON.get(BOOLEAN))
                .build());
    }

    private static JsonString jsonString(String value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonString(0);
    }

    private static JsonNumber jsonNumber(long value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonNumber(0);
    }

    private static JsonNumber jsonNumber(BigDecimal value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonNumber(0);
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
        assertThat(readValue(json, Schema.of("", expected)))
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
        assertThat(writeValue(value, Schema.of("", value)))
                .as(value.type().toString())
                .isEqualTo(expected);
    }
}
