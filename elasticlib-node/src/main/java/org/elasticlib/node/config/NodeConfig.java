/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.config;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.lineSeparator;
import java.nio.file.Path;
import java.util.Optional;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.config.ConfigException;
import org.elasticlib.common.config.ConfigReadWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node config.
 */
public final class NodeConfig {

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
     * Whether listening to multicast discovery requests is enabled. Default to true. If disabled, this node will not be
     * discoverable by other ones.
     */
    public static final String DISCOVERY_MULTICAST_LISTEN = "discovery.multicast.listen";
    /**
     * Whether discovery by multicast requests is enabled. Default to true.
     */
    public static final String DISCOVERY_MULTICAST_PING_ENABLED = "discovery.multicast.ping.enabled";
    /**
     * Multicast discovery requests interval. Default to '30 seconds'.
     */
    public static final String DISCOVERY_MULTICAST_PING_INTERVAL = "discovery.multicast.ping.interval";
    /**
     * Multicast discovery group address. Default to '235.141.20.10'. Valid multicast group addresses are in the range
     * 224.0.0.0 to 239.255.255.255, inclusive (Class D IP addresses). The address 224.0.0.0 is reserved and should not
     * be used.
     */
    public static final String DISCOVERY_MULTICAST_GROUP = "discovery.multicast.group";
    /**
     * Multicast discovery port. Default to 23875.
     */
    public static final String DISCOVERY_MULTICAST_PORT = "discovery.multicast.port";
    /**
     * Multicast discovery packets time to live. Default to 3. Must be in the range 0 to 255, inclusive. If TTL is set
     * to 0, packets are only delivered locally.
     */
    public static final String DISCOVERY_MULTICAST_TTL = "discovery.multicast.ttl";
    /**
     * Whether unicast discovery is enabled. Default to true.
     */
    public static final String DISCOVERY_UNICAST_ENABLED = "discovery.unicast.enabled";
    /**
     * Unicast discovery task scheduling interval. Default to '30 seconds'.
     */
    public static final String DISCOVERY_UNICAST_INTERVAL = "discovery.unicast.interval";
    /**
     * URI(s) of the remotes node to contact for unicast discovery. All known remotes nodes are contacted if missing.
     */
    public static final String DISCOVERY_UNICAST_URIS = "discovery.unicast.uris";
    /**
     * Whether remote nodes ping is enabled. Default to true.
     */
    public static final String REMOTES_PING_ENABLED = "remotes.ping.enabled";
    /**
     * Remote nodes exchange ping task scheduling interval. Default to '10 seconds'.
     */
    public static final String REMOTES_PING_INTERVAL = "remotes.ping.interval";
    /**
     * Whether unreachable remote nodes are automatically removed. Default to true.
     */
    public static final String REMOTES_CLEANUP_ENABLED = "remotes.cleanup.enabled";
    /**
     * Unreachable remote nodes cleanup task scheduling interval. Default to '60 seconds'.
     */
    public static final String REMOTES_CLEANUP_INTERVAL = "remotes.cleanup.interval";
    /**
     * Asynchronous tasks executor pool size. Default to the number of available processors.
     */
    public static final String TASKS_POOL_SIZE = "tasks.poolSize";
    /**
     * Maximum number of suspended content staging sessions. Default to 20.
     */
    public static final String STAGING_SESSIONS_MAX_SIZE = "staging.maxSize";
    /**
     * Suspended content staging sessions timeout. Default to '60 seconds'.
     */
    public static final String STAGING_SESSIONS_TIMEOUT = "staging.timeout";
    /**
     * Whether suspended content staging sessions are periodically cleaned. Default to true.
     */
    public static final String STAGING_SESSIONS_CLEANUP_ENABLED = "staging.cleanup.enabled";
    /**
     * Periodicity at which suspended content staging sessions cleanup is performed. Default to '30 seconds'.
     */
    public static final String STAGING_SESSIONS_CLEANUP_INTERVAL = "staging.cleanup.interval";
    /**
     * Whether deffered databases are periodically flushed. Default to true.
     */
    public static final String STORAGE_SYNC_ENABLED = "storage.sync.enabled";
    /**
     * Periodicity at which deffered databases are flushed. Default to '10 seconds'.
     */
    public static final String STORAGE_SYNC_INTERVAL = "storage.sync.interval";
    /**
     * The lock timeout for all Berkeley DB operations. Default to '0', meaning that locking never times out.
     */
    public static final String JE_LOCK_TIMEOUT = "je.lock.timeout";
    private static final Logger LOG = LoggerFactory.getLogger(NodeConfig.class);
    private static final Config DEFAULT = new Config()
            .set(HTTP_HOST, "0.0.0.0")
            .set(HTTP_PORT, 9400)
            .set(HTTP_CONTEXT, "/")
            .set(DISCOVERY_MULTICAST_LISTEN, true)
            .set(DISCOVERY_MULTICAST_PING_ENABLED, true)
            .set(DISCOVERY_MULTICAST_PING_INTERVAL, "10 minutes")
            .set(DISCOVERY_MULTICAST_GROUP, "235.141.20.10")
            .set(DISCOVERY_MULTICAST_PORT, 23875)
            .set(DISCOVERY_MULTICAST_TTL, 3)
            .set(DISCOVERY_UNICAST_ENABLED, true)
            .set(DISCOVERY_UNICAST_INTERVAL, "60 seconds")
            .set(REMOTES_PING_ENABLED, true)
            .set(REMOTES_PING_INTERVAL, "60 seconds")
            .set(REMOTES_CLEANUP_ENABLED, true)
            .set(REMOTES_CLEANUP_INTERVAL, "10 minutes")
            .set(TASKS_POOL_SIZE, getRuntime().availableProcessors())
            .set(STAGING_SESSIONS_MAX_SIZE, 20)
            .set(STAGING_SESSIONS_TIMEOUT, "60 seconds")
            .set(STAGING_SESSIONS_CLEANUP_ENABLED, true)
            .set(STAGING_SESSIONS_CLEANUP_INTERVAL, "30 seconds")
            .set(STORAGE_SYNC_ENABLED, true)
            .set(STORAGE_SYNC_INTERVAL, "10 seconds")
            .set(JE_LOCK_TIMEOUT, "0");

    private NodeConfig() {
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
