package store.common.yaml;

import com.google.common.base.Function;
import static com.google.common.io.BaseEncoding.base64;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.Instant;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.constructor.SafeConstructor.ConstructYamlBool;
import org.yaml.snakeyaml.constructor.SafeConstructor.ConstructYamlTimestamp;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;
import store.common.hash.Hash;
import store.common.value.Value;

final class ValueReading {

    private static final Map<Tag, Function<Node, Value>> READERS = new HashMap<>();
    private static final Tag HASH_TAG = new Tag("!hash");

    static {
        READERS.put(HASH_TAG, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                return Value.of(new Hash(value(node)));
            }
        });
        READERS.put(Tag.NULL, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                return Value.ofNull();
            }
        });
        READERS.put(Tag.BINARY, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                return Value.of(base64().decode(value(node)));
            }
        });
        READERS.put(Tag.BOOL, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                boolean bool = (boolean) new SafeConstructor().new ConstructYamlBool().construct(node);
                return Value.of(bool);
            }
        });
        READERS.put(Tag.INT, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                Number number = (Number) new SafeConstructor().new ConstructYamlInt().construct(node);
                long lg;
                if (number instanceof BigInteger) {
                    lg = new BigDecimal((BigInteger) number).longValueExact();
                } else {
                    lg = number.longValue();
                }
                return Value.of(lg);
            }
        });
        READERS.put(Tag.FLOAT, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                return Value.of(new BigDecimal(value(node)));
            }
        });
        READERS.put(Tag.STR, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                return Value.of(value(node));
            }
        });
        READERS.put(Tag.TIMESTAMP, new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                Date date = (Date) new ConstructYamlTimestamp().construct(node);
                return Value.of(new Instant(date));
            }
        });

        Function<Node, Value> mapReader = new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                Map<String, Value> map = new LinkedHashMap<>();
                for (NodeTuple tuple : MappingNode.class.cast(node).getValue()) {
                    String key = value(tuple.getKeyNode());
                    Value value = read(tuple.getValueNode());
                    map.put(key, value);
                }
                return Value.of(map);
            }
        };
        READERS.put(Tag.MAP, mapReader);
        READERS.put(Tag.OMAP, mapReader);

        Function<Node, Value> seqReader = new Function<Node, Value>() {
            @Override
            public Value apply(Node node) {
                List<Value> list = new ArrayList<>();
                for (Node item : SequenceNode.class.cast(node).getValue()) {
                    list.add(read(item));
                }
                return Value.of(list);
            }
        };
        READERS.put(Tag.SEQ, seqReader);
        READERS.put(Tag.SET, seqReader);
    }

    private ValueReading() {
    }

    public static Value read(Node node) {
        return READERS.get(node.getTag()).apply(node);
    }

    private static String value(Node node) {
        return ScalarNode.class.cast(node).getValue();
    }
}
