package store.common.yaml;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Node;
import store.common.Mappable;
import store.common.MappableUtil;
import store.common.value.Value;

/**
 * YAML reading utils.
 */
public final class YamlReading {

    private YamlReading() {
    }

    /**
     * Deserialize a mappable from a YAML document.
     *
     * @param <T> Actual class to deserialize to.
     * @param yaml A YAML document.
     * @param clazz Actual class to deserialize to.
     * @return Corresponding mappable instance.
     */
    public static <T extends Mappable> T read(String yaml, Class<T> clazz) {
        return MappableUtil.fromMap(read(yaml), clazz);
    }

    /**
     * Deserialize a map of values from a YAML document.
     *
     * @param yaml A YAML document.
     * @return Corresponding map of values.
     */
    public static Map<String, Value> read(String yaml) {
        return ValueReading.read(compose(yaml)).asMap();
    }

    private static Node compose(String yaml) {
        try (Reader reader = new StringReader(yaml)) {
            return new Yaml().compose(reader);

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
