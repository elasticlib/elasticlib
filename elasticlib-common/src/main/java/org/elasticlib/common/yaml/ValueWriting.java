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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import static org.elasticlib.common.value.ValueType.ARRAY;
import static org.elasticlib.common.value.ValueType.BINARY;
import static org.elasticlib.common.value.ValueType.BOOLEAN;
import static org.elasticlib.common.value.ValueType.DATE;
import static org.elasticlib.common.value.ValueType.DECIMAL;
import static org.elasticlib.common.value.ValueType.GUID;
import static org.elasticlib.common.value.ValueType.HASH;
import static org.elasticlib.common.value.ValueType.INTEGER;
import static org.elasticlib.common.value.ValueType.NULL;
import static org.elasticlib.common.value.ValueType.OBJECT;
import static org.elasticlib.common.value.ValueType.STRING;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

final class ValueWriting {

    private static final Map<ValueType, Function<Value, Node>> WRITERS = new EnumMap<>(ValueType.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

    static {
        WRITERS.put(NULL, value -> newScalarNode(Tag.NULL, "null"));
        WRITERS.put(HASH, value -> newScalarNode(Tags.HASH, value.asHash().asHexadecimalString()));
        WRITERS.put(GUID, value -> newScalarNode(Tags.GUID, value.asGuid().asHexadecimalString()));
        WRITERS.put(BINARY, value -> newScalarNode(Tag.BINARY, base64().encode(value.asByteArray())));
        WRITERS.put(BOOLEAN, value -> newScalarNode(Tag.BOOL, Boolean.toString(value.asBoolean())));
        WRITERS.put(INTEGER, value -> newScalarNode(Tag.INT, Long.toString(value.asLong())));
        WRITERS.put(DECIMAL, value -> newScalarNode(Tag.FLOAT, value.asBigDecimal().toString()));
        WRITERS.put(STRING, value -> writeString(value.asString()));
        WRITERS.put(DATE, value -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(ZoneOffset.UTC);
            return newScalarNode(Tag.TIMESTAMP, formatter.format(value.asInstant()));
        });
        WRITERS.put(OBJECT, value -> {
            List<NodeTuple> tuples = value.asMap()
                    .entrySet()
                    .stream()
                    .map(entry -> new NodeTuple(writeString(entry.getKey()), writeValue(entry.getValue())))
                    .collect(toList());

            return new MappingNode(Tag.MAP, tuples, false);
        });
        WRITERS.put(ARRAY, value -> {
            List<Node> nodes = value.asList()
                    .stream()
                    .map(item -> writeValue(item))
                    .collect(toList());

            return new SequenceNode(Tag.SEQ, nodes, false);
        });
    }

    private ValueWriting() {
    }

    /**
     * Convert a value to a YAML Node.
     *
     * @param value A value.
     * @return Corresponding YAML Node.
     */
    public static Node writeValue(Value value) {
        return WRITERS.get(value.type()).apply(value);
    }

    private static Node writeString(String str) {
        return newScalarNode(Tag.STR, str);
    }

    private static Node newScalarNode(Tag tag, String value) {
        return new ScalarNode(tag, value, null, null, null);
    }
}
