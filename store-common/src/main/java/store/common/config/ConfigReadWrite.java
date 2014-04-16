package store.common.config;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.yaml.snakeyaml.error.YAMLException;
import store.common.yaml.YamlReading;
import store.common.yaml.YamlWriting;

/**
 * Config reading and writing utils.
 */
public final class ConfigReadWrite {

    private ConfigReadWrite() {
    }

    /**
     * Reads config at supplied path. Fails if file at supplied path is not a valid YAML file. Returns nothing if file
     * does not exist or is empty.
     *
     * @param path File path.
     * @return A new config instance, if any.
     */
    public static Optional<Config> read(Path path) {
        if (!Files.exists(path)) {
            return Optional.absent();
        }
        try (InputStream inputStream = Files.newInputStream(path);
                Reader streamReader = new InputStreamReader(inputStream, Charsets.UTF_8)) {

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[1024];
            int length = streamReader.read(buffer);
            while (length > 0) {
                builder.append(buffer, 0, length);
                length = streamReader.read(buffer);
            }
            String text = builder.toString();
            if (text.isEmpty()) {
                return Optional.absent();
            }
            return Optional.of(new Config(YamlReading.readValue(text)));

        } catch (IOException | YAMLException e) {
            throw new ConfigException(e);
        }
    }

    /**
     * Write supplied config at supplied path.
     *
     * @param path File path.
     * @param config Config to write.
     */
    public static void write(Path path, Config config) {
        try (OutputStream outputStream = Files.newOutputStream(path);
                Writer writer = new OutputStreamWriter(outputStream, Charsets.UTF_8)) {

            writer.write(YamlWriting.writeValue(config.asValue()));

        } catch (IOException e) {
            throw new ConfigException(e);
        }
    }
}
