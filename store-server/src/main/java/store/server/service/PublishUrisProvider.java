package store.server.service;

import com.google.common.base.Function;
import static com.google.common.collect.Lists.transform;
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
import javax.ws.rs.core.UriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.common.value.Value;
import store.common.value.ValueType;
import store.server.config.ServerConfig;

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
        if (config.containsKey(ServerConfig.NODE_URIS)) {
            return readConfig();
        }
        return uris(hosts());
    }

    private List<URI> readConfig() {
        Value configVal = config.get(ServerConfig.NODE_URIS);
        if (configVal.type() == ValueType.STRING) {
            return singletonList(URI.create(configVal.asString()));
        }
        return transform(configVal.asList(), new Function<Value, URI>() {
            @Override
            public URI apply(Value val) {
                return URI.create(val.asString());
            }
        });
    }

    private List<String> hosts() {
        String bindHost = config.getString(ServerConfig.HTTP_HOST);
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

    private List<URI> uris(List<String> hosts) {
        return transform(hosts, new Function<String, URI>() {
            @Override
            public URI apply(String host) {
                return UriBuilder.fromUri("http:/")
                        .host(host)
                        .port(config.getInt(ServerConfig.HTTP_PORT))
                        .path(config.getString(ServerConfig.HTTP_CONTEXT))
                        .build();
            }
        });
    }
}
