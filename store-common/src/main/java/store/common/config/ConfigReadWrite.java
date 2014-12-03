package store.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.yaml.snakeyaml.error.YAMLException;
import store.common.value.Value;
import store.common.yaml.YamlReader;
import store.common.yaml.YamlWriter;

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
            return Optional.empty();
        }

        try (InputStream input = Files.newInputStream(path);
                YamlReader reader = new YamlReader(input)) {

            Optional<Value> value = reader.readValue();
            if (!value.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new Config(value.get()));

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
        try (OutputStream output = Files.newOutputStream(path);
                YamlWriter writer = new YamlWriter(output)) {

            writer.writeValue(config.asValue());

        } catch (IOException e) {
            throw new ConfigException(e);
        }
    }
}
