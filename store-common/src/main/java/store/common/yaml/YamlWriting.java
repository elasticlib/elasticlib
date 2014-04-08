package store.common.yaml;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import java.util.List;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.emitter.Emitable;
import org.yaml.snakeyaml.emitter.Emitter;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.resolver.Resolver;
import org.yaml.snakeyaml.serializer.Serializer;
import store.common.Mappable;
import store.common.value.Value;

/**
 * YAML writing utils.
 */
public final class YamlWriting {

    private static final int INDENT = 2;

    private YamlWriting() {
    }

    /**
     * Serialize a mappable as a YAML document.
     *
     * @param mappable A mappable.
     * @return Corresponding YAML document.
     */
    public static String write(Mappable mappable) {
        return writeValue(Value.of(mappable.toMap()));
    }

    /**
     * Serialize a list of mappables as a YAML document.
     *
     * @param mappables A list of mappables.
     * @return Corresponding YAML document.
     */
    public static String writeAll(List<? extends Mappable> mappables) {
        List<Value> values = new ArrayList<>();
        for (Mappable mappable : mappables) {
            values.add(Value.of(mappable.toMap()));
        }
        return writeValues(values);
    }

    /**
     * Serialize a value as a YAML document.
     *
     * @param value A value.
     * @return Corresponding YAML document.
     */
    public static String writeValue(Value value) {
        return serialize(singletonList(ValueWriting.writeValue(value)));
    }

    /**
     * Serialize a list of values as a YAML document.
     *
     * @param values A list of values.
     * @return Corresponding YAML document.
     */
    public static String writeValues(List<Value> values) {
        List<Node> nodes = new ArrayList<>();
        for (Value value : values) {
            nodes.add(ValueWriting.writeValue(value));
        }
        return serialize(nodes);
    }

    private static String serialize(List<Node> nodes) {
        DumperOptions options = new DumperOptions();
        options.setIndent(INDENT);
        if (nodes.size() > 1) {
            options.setExplicitStart(true);
            options.setExplicitEnd(true);
        }
        try (StringWriter writer = new StringWriter()) {
            Emitable emitter = new Emitter(writer, options);
            Serializer serializer = new Serializer(emitter, new Resolver(), options, null);
            try {
                serializer.open();
                for (Node node : nodes) {
                    serializer.serialize(node);
                }
                return writer.toString();

            } finally {
                serializer.close();
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
