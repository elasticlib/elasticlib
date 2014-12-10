/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.json;

import com.google.common.collect.ImmutableMap;
import static com.google.common.io.BaseEncoding.base64;
import java.math.BigDecimal;
import java.time.Instant;
import static java.util.Arrays.asList;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import static org.elasticlib.common.json.ValueReading.readValue;
import static org.elasticlib.common.json.ValueWriting.writeValue;
import org.elasticlib.common.json.schema.Schema;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import static org.elasticlib.common.value.ValueType.BOOLEAN;
import static org.elasticlib.common.value.ValueType.INTEGER;
import static org.elasticlib.common.value.ValueType.STRING;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class ValueReadingAndWritingTest {

    private static final String HASH = "8d5f3c77e94a0cad3a32340d342135f43dbb7cbb";
    private static final String GUID = "8d5f3c77e94a0cad3a32340d342135f4";
    private static final Map<ValueType, Value> VALUES = new EnumMap<>(ValueType.class);
    private static final Map<ValueType, JsonValue> JSON = new EnumMap<>(ValueType.class);

    static {
        put(Value.ofNull(),
            JsonValue.NULL);

        put(Value.of(new Hash(HASH)),
            jsonString(HASH));

        put(Value.of(new Guid(GUID)),
            jsonString(GUID));

        byte[] bytes = new Hash(HASH).getBytes();
        put(Value.of(bytes),
            jsonString(base64().encode(bytes)));

        put(Value.of(true),
            JsonValue.TRUE);

        put(Value.of(120),
            jsonNumber(120));

        put(Value.of(new BigDecimal("3.14159265359")),
            jsonNumber(new BigDecimal("3.14159265359")));

        put(Value.of("test"),
            jsonString("test"));

        put(Value.of(Instant.ofEpochMilli(1391878872)),
            jsonNumber(1391878872));

        put(Value.of(ImmutableMap.of("num", VALUES.get(INTEGER),
                                     "text", VALUES.get(STRING),
                                     "bool", VALUES.get(BOOLEAN))),
            createObjectBuilder()
            .add("num", JSON.get(INTEGER))
            .add("text", JSON.get(STRING))
            .add("bool", JSON.get(BOOLEAN))
            .build());

        put(Value.of(asList(VALUES.get(INTEGER),
                            VALUES.get(STRING),
                            VALUES.get(BOOLEAN))),
            createArrayBuilder()
            .add(JSON.get(INTEGER))
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
        Set<ValueType> types = VALUES.keySet();
        Object[][] data = new Object[types.size()][];
        int i = 0;
        for (ValueType type : types) {
            data[i] = new Object[]{JSON.get(type), VALUES.get(type)};
            i++;
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
        Set<ValueType> types = VALUES.keySet();
        Object[][] data = new Object[types.size()][];
        int i = 0;
        for (ValueType type : types) {
            data[i] = new Object[]{VALUES.get(type), JSON.get(type)};
            i++;
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
