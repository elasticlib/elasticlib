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
package org.elasticlib.common.yaml;

import static com.google.common.io.BaseEncoding.base64;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import static org.elasticlib.common.yaml.ValueReading.read;
import static org.elasticlib.common.yaml.ValueWriting.writeValue;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * Unit tests.
 */
public class ValueReadingAndWritingTest {

    private static final String HASH = "8d5f3c77e94a0cad3a32340d342135f43dbb7cbb";
    private static final String GUID = "8d5f3c77e94a0cad3a32340d342135f4";
    private static final Map<ValueType, Value> VALUES = new EnumMap<>(ValueType.class);
    private static final Map<ValueType, ScalarNode> NODES = new EnumMap<>(ValueType.class);

    static {
        put(Value.ofNull(),
            newScalarNode(Tag.NULL, "null"));

        put(Value.of(new Hash(HASH)),
            newScalarNode(Tags.HASH, HASH));

        put(Value.of(new Guid(GUID)),
            newScalarNode(Tags.GUID, GUID));

        byte[] bytes = new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb").getBytes();
        put(Value.of(bytes),
            newScalarNode(Tag.BINARY, base64().encode(bytes)));

        put(Value.of(true),
            newScalarNode(Tag.BOOL, "true"));

        put(Value.of(42),
            newScalarNode(Tag.INT, "42"));

        String pi = "3.14159265359";
        put(Value.of(new BigDecimal(pi)),
            newScalarNode(Tag.FLOAT, pi));

        String text = "test";
        put(Value.of(text),
            newScalarNode(Tag.STR, text));

        put(Value.of(Instant.ofEpochMilli(1391878000)),
            newScalarNode(Tag.TIMESTAMP, "1970-01-17T02:37:58.000Z"));
    }

    private static ScalarNode newScalarNode(Tag tag, String value) {
        return new ScalarNode(tag, value, null, null, null);
    }

    private static void put(Value value, ScalarNode yaml) {
        ValueType type = value.type();
        VALUES.put(type, value);
        NODES.put(type, yaml);
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
            data[i] = new Object[]{NODES.get(type), VALUES.get(type)};
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
            data[i] = new Object[]{VALUES.get(type), NODES.get(type)};
            i++;
        }
        return data;
    }

    /**
     * Test.
     *
     * @param yaml Input data.
     * @param expected Expected Output data.
     */
    @Test(dataProvider = "readValueDataProvider")
    public void readValueTest(ScalarNode yaml, Value expected) {
        assertThat(read(yaml))
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
    public void writeValueTest(Value value, ScalarNode expected) {
        ScalarNode actual = (ScalarNode) writeValue(value);

        assertThat(actual.getTag())
                .as(value.type().toString())
                .isEqualTo(expected.getTag());

        assertThat(actual.getValue())
                .as(value.type().toString())
                .isEqualTo(expected.getValue());
    }
}
