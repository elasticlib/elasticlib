package store.server.config;

import com.google.common.base.Optional;
import static java.lang.System.lineSeparator;
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
     * Node name. Generated at runtime if missing.
     */
    public static final String NODE_NAME = "node.name";
    /**
     * URI(s) this node will publish itself in the cluster so that remote nodes may connect to it. Generated at runtime
     * if missing.
     */
    public static final String NODE_URIS = "node.uris";
    /**
     * Node bind host. Defaults to '0.0.0.0'.
     */
    public static final String HTTP_HOST = "http.host";
    /**
     * Node TCP port. Default to 9400.
     */
    public static final String HTTP_PORT = "http.port";
    /**
     * Node deployment context. Default to '/'.
     */
    public static final String HTTP_CONTEXT = "http.context";
    /**
     * Whether multicast discovery is enabled. Default to true.
     */
    public static final String DISCOVERY_ENABLE = "discovery.enable";
    /**
     * Multicast discovery group address. Default to '235.141.20.10'. Valid multicast group addresses are in the range
     * 224.0.0.0 to 239.255.255.255, inclusive (Class D IP addresses). The address 224.0.0.0 is reserved and should not
     * be used.
     */
    public static final String DISCOVERY_GROUP = "discovery.group";
    /**
     * Multicast discovery port. Default to 23875.
     */
    public static final String DISCOVERY_PORT = "discovery.port";
    /**
     * Multicast discovery packets time to live. Default to 3. Must be in the range 0 to 255, inclusive. If TTL is set
     * to 0, packets are only delivered locally.
     */
    public static final String DISCOVERY_TTL = "discovery.ttl";
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
            .set(HTTP_HOST, "0.0.0.0")
            .set(HTTP_PORT, 9400)
            .set(HTTP_CONTEXT, "/")
            .set(DISCOVERY_ENABLE, true)
            .set(DISCOVERY_GROUP, "235.141.20.10")
            .set(DISCOVERY_PORT, 23875)
            .set(DISCOVERY_TTL, 3)
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
            LOG.warn("Failed to load config at {}{}{}", path, lineSeparator(), e.getMessage());
        }
        return config;
    }
}
