package store.server.config;

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
     * Periodic tasks executor pool size. Default to 1.
     */
    public static final String ASYNC_POOL_SIZE = "async.poolSize";
    /**
     * Periodicity at which deffered databases are flushed. Default to '10 seconds'.
     */
    public static final String STORAGE_SYNC_PERIOD = "storage.syncPeriod";
    /**
     * Maximum number of suspended transactions. Default to 20.
     */
    public static final String STORAGE_SUSPENDED_TXN_MAX_SIZE = "storage.suspendedTransactions.maxSize";
    /**
     * Suspended transactions timeout. Default to '60 seconds'.
     */
    public static final String STORAGE_SUSPENDED_TXN_TIMEOUT = "storage.suspendedTransactions.timeout";
    /**
     * Suspended transactions cleanup period. Default to '30 seconds'.
     */
    public static final String STORAGE_SUSPENDED_TXN_CLEANUP_PERIOD = "storage.suspendedTransactions.cleanupPeriod";
    /**
     * The lock timeout for all Berkeley DB operations. Default to '0', meaning that locking never times out.
     */
    public static final String JE_LOCK_TIMEOUT = "je.lock.timeout";
    private static final Logger LOG = LoggerFactory.getLogger(ServerConfig.class);
    private static final Config DEFAULT = new Config()
            .set(WEB_HOST, "localhost")
            .set(WEB_PORT, 8080)
            .set(WEB_CONTEXT, "/")
            .set(ASYNC_POOL_SIZE, 1)
            .set(STORAGE_SYNC_PERIOD, "10 seconds")
            .set(STORAGE_SUSPENDED_TXN_MAX_SIZE, 10)
            .set(STORAGE_SUSPENDED_TXN_TIMEOUT, "60 seconds")
            .set(STORAGE_SUSPENDED_TXN_CLEANUP_PERIOD, "30 seconds")
            .set(JE_LOCK_TIMEOUT, "0");

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
