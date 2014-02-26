package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.util.Objects.requireNonNull;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hold server deployement config.
 */
public class ServerConfig {

    /**
     * Server host. Defaults to 'localhost'.
     */
    public static final String WEB_HOST = "web.host";
    /**
     * Server port. Defaults to '8080'.
     */
    public static final String WEB_PORT = "web.port";
    /**
     * Server context path. Defaults to '/'.
     */
    public static final String WEB_CONTEXT = "web.context";
    /**
     * Whether HTTP requests and responses must be logged (true/false). Defaults to 'true'.
     */
    public static final String LOG_LOGGING_FILTER = "log.loggingFilter";
    /**
     * Whether HTTP entities must be logged (true/false). Non applicable if HTTP requests and responses are not logged.
     * Defaults to 'true'.
     */
    public static final String LOG_PRINT_ENTITY = "log.printEntity";
    /**
     * Max entity size to print to log (in bytes). Non applicable if HTTP entities are not logged. Defaults to '8192'.
     */
    public static final String LOG_MAX_ENTITY_SIZE = "log.maxEntitySize";
    private static final Logger LOG = LoggerFactory.getLogger(ServerConfig.class);
    private static final Properties DEFAULT = new Properties();

    static {
        DEFAULT.put(WEB_HOST, "localhost");
        DEFAULT.put(WEB_PORT, "8080");
        DEFAULT.put(WEB_CONTEXT, "/");
        DEFAULT.put(LOG_LOGGING_FILTER, "true");
        DEFAULT.put(LOG_PRINT_ENTITY, "true");
        DEFAULT.put(LOG_MAX_ENTITY_SIZE, "8192");
    }
    private final Properties properties;

    /**
     * Constructor.
     *
     * @param path Properties file path.
     */
    public ServerConfig(Path path) {
        properties = new Properties(DEFAULT);
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);

        } catch (IOException e) {
            LOG.warn("Failed to load server properties from file: " + path, e);
        }
        Set<String> keySet = new TreeSet<>();
        keySet.addAll(properties.stringPropertyNames());
        keySet.addAll(DEFAULT.stringPropertyNames());
        StringBuilder builder = new StringBuilder();
        for (String key : keySet) {
            builder.append(key)
                    .append('=')
                    .append(properties.get(key))
                    .append(System.lineSeparator());
        }
        LOG.info("Using server config:" + System.lineSeparator() + builder.toString());
    }

    /**
     * Provides the string value associated to supplied key. Fails if value does not exists.
     *
     * @param key the property key.
     * @return the value associated to this property key.
     */
    public String getString(String key) {
        return requireNonNull(properties.getProperty(key), "Missing property: " + key);
    }

    /**
     * Provides the boolean value associated to supplied key. Fails if value does not exists or is not a boolean.
     *
     * @param key the property key.
     * @return the value associated to this property key.
     */
    public boolean getBoolean(String key) {
        String str = getString(key);
        if (!str.equalsIgnoreCase("true") && !str.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("Property " + key + " is expected to have a boolean value");
        }
        return Boolean.parseBoolean(str);
    }

    /**
     * Provides the int value associated to supplied key. Fails if value does not exists or can not be parsed as an
     * integer.
     *
     * @param key the property key.
     * @return the value associated to this property key.
     */
    public int getInt(String key) {
        String str = getString(key);
        try {
            return Integer.parseInt(str);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Property " + key + "is expected to have an integer value", e);
        }
    }
}
