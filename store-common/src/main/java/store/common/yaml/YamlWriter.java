package store.common.yaml;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import static com.google.common.collect.Lists.transform;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import static java.util.Collections.singletonList;
import java.util.List;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.emitter.Emitable;
import org.yaml.snakeyaml.emitter.Emitter;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.resolver.Resolver;
import org.yaml.snakeyaml.serializer.Serializer;
import store.common.mappable.Mappable;
import store.common.value.Value;

/**
 * Writes Values and Mappables as YAML on a given output-stream / writer.
 */
public class YamlWriter implements AutoCloseable {

    private static final int INDENT = 2;
    private final Writer writer;

    /**
     * Constructor for a an output-stream. Internally, an UTF-8 writer is created to write to the stream.
     *
     * @param outputStream The output-stream to write to.
     */
    public YamlWriter(OutputStream outputStream) {
        this.writer = new BufferedWriter(new OutputStreamWriter(outputStream, Charsets.UTF_8));
    }

    /**
     * Constructor for a writer.
     *
     * @param writer The writer to write to.
     */
    public YamlWriter(Writer writer) {
        this.writer = writer;
    }

    /**
     * Writes a mappable as a YAML document.
     *
     * @param mappable A mappable.
     * @throws IOException If an IO error happens on the underlying stream.
     */
    public void write(Mappable mappable) throws IOException {
        writeValue(Value.of(mappable.toMap()));
    }

    /**
     * Writes a list of mappables as a YAML document.
     *
     * @param mappables A list of mappables.
     * @throws IOException If an IO error happens on the underlying stream.
     */
    public void writeAll(List<? extends Mappable> mappables) throws IOException {
        writeValues(transform(mappables, new Function<Mappable, Value>() {
            @Override
            public Value apply(Mappable mappable) {
                return Value.of(mappable.toMap());
            }
        }));
    }

    /**
     * Writes a value as a YAML document.
     *
     * @param value A value.
     * @throws IOException If an IO error happens on the underlying stream.
     */
    public void writeValue(Value value) throws IOException {
        write(singletonList(ValueWriting.writeValue(value)));
    }

    /**
     * Writes a list of values as a YAML document.
     *
     * @param values A list of values.
     * @throws IOException If an IO error happens on the underlying stream.
     */
    public void writeValues(List<Value> values) throws IOException {
        write(transform(values, new Function<Value, Node>() {
            @Override
            public Node apply(Value value) {
                return ValueWriting.writeValue(value);
            }
        }));
    }

    /**
     * Writes a value as a YAML document to a string.
     *
     * @param value A value.
     * @return Corresponding YAML document as a string.
     */
    public static String writeToString(Value value) {
        try (Writer stringWriter = new StringWriter();
                YamlWriter yamlWriter = new YamlWriter(stringWriter)) {

            yamlWriter.writeValue(value);
            return stringWriter.toString();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void write(List<Node> nodes) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setIndent(INDENT);
        if (nodes.size() > 1) {
            options.setExplicitStart(true);
            options.setExplicitEnd(true);
        }

        Emitable emitter = new Emitter(writer, options);
        Serializer serializer = new Serializer(emitter, new Resolver(), options, null);
        try {
            serializer.open();
            for (Node node : nodes) {
                serializer.serialize(node);
            }

        } finally {
            serializer.close();
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
