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

import static java.lang.System.lineSeparator;
import java.nio.file.Path;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.config.ConfigException;
import org.elasticlib.common.config.ConfigReadWrite;
import static org.elasticlib.common.config.ConfigReadWrite.readFromClassPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node config.
 */
public final class NodeConfig {

    /**
     * Node name. Generated at runtime if empty.
     */
    public static final String NODE_NAME = "node.name";
    /**
     * URI(s) this node will publish itself in the cluster so that remote nodes may connect to it. Generated at runtime
     * if empty.
     */
    public static final String NODE_URIS = "node.uris";
    /**
     * Node bind host.
     */
    public static final String HTTP_HOST = "http.host";
    /**
     * Node TCP port.
     */
    public static final String HTTP_PORT = "http.port";
    /**
     * Node deployment context.
     */
    public static final String HTTP_CONTEXT = "http.context";
    /**
     * Whether listening to multicast discovery requests is enabled. If disabled, this node will not be discoverable by
     * other ones.
     */
    public static final String DISCOVERY_MULTICAST_LISTEN = "discovery.multicast.listen";
    /**
     * Whether discovery by multicast requests is enabled.
     */
    public static final String DISCOVERY_MULTICAST_PING_ENABLED = "discovery.multicast.ping.enabled";
    /**
     * Multicast discovery requests interval.
     */
    public static final String DISCOVERY_MULTICAST_PING_INTERVAL = "discovery.multicast.ping.interval";
    /**
     * Multicast discovery group address. Valid multicast group addresses are in the range 224.0.0.0 to 239.255.255.255,
     * inclusive (Class D IP addresses). The address 224.0.0.0 is reserved and should not be used.
     */
    public static final String DISCOVERY_MULTICAST_GROUP = "discovery.multicast.group";
    /**
     * Multicast discovery port.
     */
    public static final String DISCOVERY_MULTICAST_PORT = "discovery.multicast.port";
    /**
     * Multicast discovery packets time to live. Must be in the range 0 to 255, inclusive. If TTL is set to 0, packets
     * are only delivered locally.
     */
    public static final String DISCOVERY_MULTICAST_TTL = "discovery.multicast.ttl";
    /**
     * Whether unicast discovery is enabled.
     */
    public static final String DISCOVERY_UNICAST_ENABLED = "discovery.unicast.enabled";
    /**
     * Unicast discovery task scheduling interval.
     */
    public static final String DISCOVERY_UNICAST_INTERVAL = "discovery.unicast.interval";
    /**
     * URI(s) of the remotes node to contact for unicast discovery. If empty, all known remotes are contacted.
     */
    public static final String DISCOVERY_UNICAST_URIS = "discovery.unicast.uris";
    /**
     * Whether remote nodes ping is enabled.
     */
    public static final String REMOTES_PING_ENABLED = "remotes.ping.enabled";
    /**
     * Remote nodes exchange ping task scheduling interval.
     */
    public static final String REMOTES_PING_INTERVAL = "remotes.ping.interval";
    /**
     * Whether unreachable remote nodes are automatically removed.
     */
    public static final String REMOTES_CLEANUP_ENABLED = "remotes.cleanup.enabled";
    /**
     * Unreachable remote nodes cleanup task scheduling interval.
     */
    public static final String REMOTES_CLEANUP_INTERVAL = "remotes.cleanup.interval";
    /**
     * Asynchronous tasks executor pool size.
     */
    public static final String TASKS_POOL_SIZE = "tasks.poolSize";
    /**
     * Maximum number of suspended content staging sessions.
     */
    public static final String STAGING_SESSIONS_MAX_SIZE = "staging.maxSize";
    /**
     * Suspended content staging sessions timeout.
     */
    public static final String STAGING_SESSIONS_TIMEOUT = "staging.timeout";
    /**
     * Whether suspended content staging sessions are periodically cleaned.
     */
    public static final String STAGING_SESSIONS_CLEANUP_ENABLED = "staging.cleanup.enabled";
    /**
     * Periodicity at which suspended content staging sessions cleanup is performed.
     */
    public static final String STAGING_SESSIONS_CLEANUP_INTERVAL = "staging.cleanup.interval";
    /**
     * Whether deffered databases are periodically flushed.
     */
    public static final String STORAGE_SYNC_ENABLED = "storage.sync.enabled";
    /**
     * Periodicity at which deffered databases are flushed.
     */
    public static final String STORAGE_SYNC_INTERVAL = "storage.sync.interval";
    /**
     * The lock timeout for all Berkeley DB operations. '0' means that locking never times out.
     */
    public static final String JE_LOCK_TIMEOUT = "je.lock.timeout";

    private static final Logger LOG = LoggerFactory.getLogger(NodeConfig.class);

    private NodeConfig() {
    }

    /**
     * Provides the node default config, extended with config loaded from supplied path.
     *
     * @param path Config file path.
     * @return A new config instance.
     */
    public static Config load(Path path) {
        Config config = readFromClassPath(NodeConfig.class, "config.yml");
        try {
            return config.extend(ConfigReadWrite.read(path));

        } catch (ConfigException e) {
            LOG.warn("Failed to load config at {}{}{}", path, lineSeparator(), e.getMessage());
        }
        return config;
    }
}
