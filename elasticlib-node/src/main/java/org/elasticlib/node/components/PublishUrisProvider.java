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
package org.elasticlib.node.components;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import static java.net.NetworkInterface.getNetworkInterfaces;
import java.net.SocketException;
import java.net.URI;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import java.util.Enumeration;
import java.util.List;
import static java.util.stream.Collectors.toList;
import javax.ws.rs.core.UriBuilder;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.config.ConfigUtil;
import org.elasticlib.node.config.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides local node publish URI(s).
 */
public class PublishUrisProvider {

    private static final String ANY_LOCAL_HOST = "0.0.0.0";
    private static final String LOCALHOST = "localhost";
    private static final Logger LOG = LoggerFactory.getLogger(PublishUrisProvider.class);

    private final Config config;

    /**
     * Constructor.
     *
     * @param config Config.
     */
    public PublishUrisProvider(Config config) {
        this.config = config;
    }

    /**
     * Reads the publish URI(s) of the local node in the configuration. If this property is not statically defined in
     * the configuration, builds publish URI(s) according to the HTTP bind host :<br>
     * - If bind host is '0.0.0.0', uses all non-loopback addresses found or localhost as a fallback.<br>
     * - Otherwise, uses bind host as it.
     *
     * @return The publish URI(s) of the local node.
     */
    public List<URI> uris() {
        List<URI> uris = ConfigUtil.uris(config, NodeConfig.NODE_URIS);
        if (!uris.isEmpty()) {
            return uris;
        }
        return hosts()
                .stream()
                .map(this::toUri)
                .collect(toList());
    }

    private URI toUri(String host) {
        return UriBuilder.fromUri("http:/")
                .host(host)
                .port(config.getInt(NodeConfig.HTTP_PORT))
                .path(config.getString(NodeConfig.HTTP_API_PATH))
                .build();
    }

    private List<String> hosts() {
        String bindHost = config.getString(NodeConfig.HTTP_HOST);
        if (!bindHost.equals(ANY_LOCAL_HOST)) {
            return singletonList(bindHost);
        }
        List<String> hosts = nonLoopBackHosts();
        if (hosts.isEmpty()) {
            return singletonList(LOCALHOST);
        }
        return hosts;
    }

    private static List<String> nonLoopBackHosts() {
        List<String> hosts = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
                        hosts.add(address.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            LOG.warn("Failed to list network interfaces", e);
        }
        sort(hosts);
        return hosts;
    }

}
