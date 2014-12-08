package org.elasticlib.node.service;

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
        if (config.containsKey(NodeConfig.NODE_URIS)) {
            return ConfigUtil.uris(config, NodeConfig.NODE_URIS);
        }
        return hosts().stream().map(this::uri).collect(toList());
    }

    private URI uri(String host) {
        return UriBuilder.fromUri("http:/")
                .host(host)
                .port(config.getInt(NodeConfig.HTTP_PORT))
                .path(config.getString(NodeConfig.HTTP_CONTEXT))
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
