package org.elasticlib.common.yaml;

import static com.google.common.io.BaseEncoding.base64;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.value.Value;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.constructor.SafeConstructor.ConstructYamlTimestamp;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

final class ValueReading {

    private static final Map<Tag, Function<Node, Value>> READERS = new HashMap<>();

    static {
        READERS.put(Tag.NULL, node -> Value.ofNull());
        READERS.put(Tags.HASH, node -> Value.of(new Hash(value(node))));
        READERS.put(Tags.GUID, node -> Value.of(new Guid(value(node))));
        READERS.put(Tag.BINARY, node -> Value.of(base64().decode(value(node))));
        READERS.put(Tag.BOOL, node -> {
            boolean bool = (boolean) new SafeConstructor().new ConstructYamlBool().construct(node);
            return Value.of(bool);
        });
        READERS.put(Tag.INT, node -> {
            Number number = (Number) new SafeConstructor().new ConstructYamlInt().construct(node);
            long lg;
            if (number instanceof BigInteger) {
                lg = new BigDecimal((BigInteger) number).longValueExact();
            } else {
                lg = number.longValue();
            }
            return Value.of(lg);
        });
        READERS.put(Tag.FLOAT, node -> Value.of(new BigDecimal(value(node))));
        READERS.put(Tag.STR, node -> Value.of(value(node)));
        READERS.put(Tag.TIMESTAMP, node -> {
            Date date = (Date) new ConstructYamlTimestamp().construct(node);
            return Value.of(date.toInstant());
        });

        Function<Node, Value> mapReader = node -> {
            Map<String, Value> map = new LinkedHashMap<>();
            MappingNode.class.cast(node).getValue().forEach(tuple -> {
                String key = value(tuple.getKeyNode());
                Value value = read(tuple.getValueNode());
                map.put(key, value);
            });
            return Value.of(map);
        };
        READERS.put(Tag.MAP, mapReader);
        READERS.put(Tag.OMAP, mapReader);

        Function<Node, Value> seqReader = node -> {
            List<Value> list = SequenceNode.class.cast(node)
                    .getValue()
                    .stream()
                    .map(item -> read(item))
                    .collect(toList());

            return Value.of(list);
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
