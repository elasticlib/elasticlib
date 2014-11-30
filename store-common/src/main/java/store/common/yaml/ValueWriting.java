package store.common.yaml;

import static com.google.common.io.BaseEncoding.base64;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import static java.util.stream.Collectors.toList;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.DECIMAL;
import static store.common.value.ValueType.GUID;
import static store.common.value.ValueType.HASH;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

final class ValueWriting {

    private static final Map<ValueType, Function<Value, Node>> WRITERS = new EnumMap<>(ValueType.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

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
            DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_FORMAT);
            return newScalarNode(Tag.TIMESTAMP, formatter.print(value.asInstant()));
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
