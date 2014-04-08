package store.common.yaml;

import com.google.common.base.Function;
import static com.google.common.io.BaseEncoding.base64;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import static store.common.value.ValueType.HASH;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

final class ValueWriting {

    private static final Map<ValueType, Function<Value, Node>> WRITERS = new EnumMap<>(ValueType.class);
    private static final Tag HASH_TAG = new Tag("!hash");
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    static {
        WRITERS.put(NULL, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return newScalarNode(Tag.NULL, "null");
            }
        });
        WRITERS.put(HASH, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return newScalarNode(HASH_TAG, value.asHash().asHexadecimalString());
            }
        });
        WRITERS.put(BINARY, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return newScalarNode(Tag.BINARY, base64().encode(value.asByteArray()));
            }
        });
        WRITERS.put(BOOLEAN, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return newScalarNode(Tag.BOOL, Boolean.toString(value.asBoolean()));
            }
        });
        WRITERS.put(INTEGER, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return newScalarNode(Tag.INT, Long.toString(value.asLong()));
            }
        });
        WRITERS.put(DECIMAL, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return newScalarNode(Tag.FLOAT, value.asBigDecimal().toString());
            }
        });
        WRITERS.put(STRING, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return writeString(value.asString());
            }
        });
        WRITERS.put(DATE, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_FORMAT);
                return newScalarNode(Tag.TIMESTAMP, formatter.print(value.asInstant()));
            }
        });
        WRITERS.put(OBJECT, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                List<NodeTuple> tuples = new ArrayList<>();
                for (Entry<String, Value> entry : value.asMap().entrySet()) {
                    tuples.add(new NodeTuple(writeString(entry.getKey()),
                                             writeValue(entry.getValue())));
                }
                return new MappingNode(Tag.MAP, tuples, false);
            }
        });
        WRITERS.put(ARRAY, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                List<Node> nodes = new ArrayList<>();
                for (Value item : value.asList()) {
                    nodes.add(writeValue(item));
                }
                return new SequenceNode(Tag.SEQ, nodes, false);
            }
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
