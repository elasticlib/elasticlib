package store.common.yaml;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
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
        return write(mappable.toMap());
    }

    /**
     * Serialize a map of values as a YAML document.
     *
     * @param map A map of values.
     * @return Corresponding YAML document.
     */
    public static String write(Map<String, Value> map) {
        return serialize(ValueWriting.writeMap(map));
    }

    private static String serialize(Node node) {
        DumperOptions options = new DumperOptions();
        options.setIndent(INDENT);

        try (StringWriter writer = new StringWriter()) {
            Emitable emitter = new Emitter(writer, options);
            Serializer serializer = new Serializer(emitter, new Resolver(), options, null);
            try {
                serializer.open();
                serializer.serialize(node);
                return writer.toString();

            } finally {
                serializer.close();
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
