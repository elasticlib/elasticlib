package store.common.yaml;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static java.util.stream.Collectors.toList;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Node;
import store.common.mappable.Mappable;
import store.common.mappable.MappableUtil;
import store.common.value.Value;

/**
 * Reads Values and Mappables as YAML from a given input-stream / reader.
 */
public class YamlReader implements AutoCloseable {

    private final Reader reader;

    /**
     * Constructor for a an input-stream. Internally, an UTF-8 reader is created to read to the stream.
     *
     * @param inputStream The input-stream to read to.
     */
    public YamlReader(InputStream inputStream) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
    }

    /**
     * Constructor for a reader.
     *
     * @param reader The reader to read from.
     */
    public YamlReader(Reader reader) {
        this.reader = reader;
    }

    /**
     * Reads a mappable.
     *
     * @param <T> Actual class to deserialize to.
     * @param clazz Actual class to deserialize to.
     * @return A new mappable instance or nothing if the end of the underlying stream has been reached.
     */
    public <T extends Mappable> Optional<T> read(Class<T> clazz) {
        Optional<Value> value = readValue();
        if (!value.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(MappableUtil.fromMap(value.get().asMap(), clazz));
    }

    /**
     * Reads a list of mappables.
     *
     * @param <T> Actual class to deserialize to.
     * @param clazz Actual class to deserialize to.
     * @return A list of mappable instances.
     */
    public <T extends Mappable> List<T> readAll(Class<T> clazz) {
        return readValues()
                .stream()
                .map(value -> MappableUtil.fromMap(value.asMap(), clazz))
                .collect(toList());
    }

    /**
     * Reads a value.
     *
     * @return A value or nothing if the end of the underlying stream has been reached.
     */
    public Optional<Value> readValue() {
        Node node = new Yaml().compose(reader);
        if (node == null) {
            return Optional.empty();
        }
        return Optional.of(ValueReading.read(node));
    }

    /**
     * Reads a list of values.
     *
     * @return Corresponding list of values.
     */
    public List<Value> readValues() {
        List<Value> list = new ArrayList<>();
        for (Node node : new Yaml().composeAll(reader)) {
            list.add(ValueReading.read(node));
        }
        return list;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
