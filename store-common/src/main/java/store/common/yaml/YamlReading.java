package store.common.yaml;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
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
        return MappableUtil.fromMap(readValue(yaml).asMap(), clazz);
    }

    /**
     * Deserialize a list of mappables from a YAML document.
     *
     * @param <T> Actual class to deserialize to.
     * @param yaml A YAML document.
     * @param clazz Actual class to deserialize to.
     * @return Corresponding list of mappable instances.
     */
    public static <T extends Mappable> List<T> readAll(String yaml, Class<T> clazz) {
        List<T> list = new ArrayList<>();
        for (Value value : readValues(yaml)) {
            list.add(MappableUtil.fromMap(value.asMap(), clazz));
        }
        return list;
    }

    /**
     * Deserialize a value from a YAML document.
     *
     * @param yaml A YAML document.
     * @return Corresponding value.
     */
    public static Value readValue(String yaml) {
        try (Reader reader = new StringReader(yaml)) {
            return ValueReading.read(new Yaml().compose(reader));

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Deserialize a list of values from a YAML document.
     *
     * @param yaml A YAML document.
     * @return Corresponding list of values.
     */
    public static List<Value> readValues(String yaml) {
        try (Reader reader = new StringReader(yaml)) {
            List<Value> list = new ArrayList<>();
            for (Node node : new Yaml().composeAll(reader)) {
                list.add(ValueReading.read(node));
            }
            return list;

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
