package store.server;

import com.google.common.base.Optional;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.common.config.ConfigException;
import store.common.config.ConfigReadWrite;

/**
 * Server config.
 */
public final class ServerConfig {

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
    private static final Config DEFAULT = new Config()
            .set(WEB_HOST, "localhost")
            .set(WEB_PORT, 8080)
            .set(WEB_CONTEXT, "/")
            .set(LOG_LOGGING_FILTER, true)
            .set(LOG_PRINT_ENTITY, true)
            .set(LOG_MAX_ENTITY_SIZE, 8192);

    private ServerConfig() {
    }

    /**
     * Load config.
     *
     * @param path Config file path.
     * @return A new config instance.
     */
    public static Config load(Path path) {
        Config config = DEFAULT;
        try {
            Optional<Config> loaded = ConfigReadWrite.read(path);
            if (loaded.isPresent()) {
                config = DEFAULT.extend(loaded.get());
            }
        } catch (ConfigException e) {
            LOG.warn("Failed to load config at " + path + System.lineSeparator() + e.getMessage());
        }
        LOG.info("Using server config:" + System.lineSeparator() + config);
        return config;
    }
}
