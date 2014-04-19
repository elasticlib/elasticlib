package store.client.config;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import store.client.exception.RequestFailedException;
import store.common.config.Config;
import store.common.config.ConfigException;
import store.common.config.ConfigReadWrite;

/**
 * Client config.
 */
public class ClientConfig {

    private static final String DEFAULT_CONNECTION = "default.connection";
    private static final String DEFAULT_REPOSITORY = "default.repository";
    private static final String DISPLAY_FORMAT = "display.format";
    private static final String DISPLAY_PRETTY = "display.pretty";
    private static final Path HOME_PATH = Paths.get(System.getProperty("user.home"), ".storeconfig.yml");
    private static final Config DEFAULT = new Config()
            .set(DEFAULT_CONNECTION, "")
            .set(DEFAULT_REPOSITORY, "")
            .set(DISPLAY_FORMAT, Format.YAML.toString())
            .set(DISPLAY_PRETTY, true);
    private Config extended;
    private Config config;

    /**
     * Initialisation. Actually load config.
     */
    public void init() {
        try {
            Optional<Config> loaded = ConfigReadWrite.read(HOME_PATH);
            if (loaded.isPresent()) {
                List<String> validKeys = listKeys();
                for (String key : loaded.get().listKeys()) {
                    if (!validKeys.contains(key)) {
                        throw new ConfigException(undefinedConfigKey(key));
                    }
                }
                config = loaded.get();

            } else {
                config = new Config();
            }
            extended = DEFAULT.extend(config);

        } catch (ConfigException e) {
            config = new Config();
            extended = DEFAULT.extend(config);
            throw new RequestFailedException("Could not load config, using default. Cause:", e);
        }
    }

    /**
     * @return All valid config keys.
     */
    public static List<String> listKeys() {
        return DEFAULT.listKeys();
    }

    /**
     * @return A printable view of this config.
     */
    public String print() {
        return extended.toString();
    }

    /**
     * @return Default server to connect to. May be empty.
     */
    public String getDefaultConnection() {
        return extended.getString(DEFAULT_CONNECTION);
    }

    /**
     * @return Default repository to use. May be empty.
     */
    public String getDefaultRepository() {
        return extended.getString(DEFAULT_REPOSITORY);
    }

    /**
     * @return Used format for entities rendering.
     */
    public Format getDisplayFormat() {
        return Format.fromString(extended.getString(DISPLAY_FORMAT));
    }

    /**
     * @return If displayed values should be nicely formatted.
     */
    public boolean isDisplayPretty() {
        return extended.getBoolean(DISPLAY_PRETTY);
    }

    /**
     * Set a config key.
     *
     * @param key Key.
     * @param value Value.
     */
    public void set(String key, String value) {
        switch (key) {
            case DEFAULT_CONNECTION:
            case DEFAULT_REPOSITORY:
                config = config.set(key, value);
                break;

            case DISPLAY_FORMAT:
                config = config.set(key, checkFormat(value));
                break;

            case DISPLAY_PRETTY:
                config = config.set(key, asBoolean(value));
                break;

            default:
                throw new RequestFailedException(undefinedConfigKey(key));
        }
        save();
    }

    private static String checkFormat(String value) {
        if (!Format.isSupported(value)) {
            throw new RequestFailedException(expectedValidFormat());
        }
        return value;
    }

    private static boolean asBoolean(String value) {
        switch (value.toLowerCase()) {
            case "true":
            case "on":
            case "yes":
                return true;

            case "false":
            case "off":
            case "no":
                return false;

            default:
                throw new RequestFailedException(expectedBooleanValue());
        }
    }

    /**
     * Unset a config key.
     *
     * @param key Key.
     */
    public void unset(String key) {
        if (!listKeys().contains(key)) {
            throw new RequestFailedException(undefinedConfigKey(key));
        }
        config = config.unset(key);
        save();
    }

    /**
     * Reset config to its default values.
     */
    public void reset() {
        try {
            Files.deleteIfExists(HOME_PATH);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
        config = new Config();
        extended = DEFAULT.extend(config);
    }

    private void save() {
        extended = DEFAULT.extend(config);
        if (!config.isEmpty()) {
            ConfigReadWrite.write(HOME_PATH, config);
        }
    }

    private static String undefinedConfigKey(String key) {
        return "Undefined config key '" + key + "'";
    }

    private static String expectedBooleanValue() {
        return "Expected a boolean value";
    }

    private static String expectedValidFormat() {
        return "Expected " + Joiner.on('|').join(Format.values());
    }
}
