package store.server.service;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import static com.google.common.collect.Lists.transform;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import static java.net.NetworkInterface.getNetworkInterfaces;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import java.util.Enumeration;
import java.util.List;
import javax.json.JsonObject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.jersey.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.NodeDef;
import store.common.config.Config;
import store.common.hash.Guid;
import static store.common.json.JsonReading.read;
import store.common.value.Value;
import store.common.value.ValueType;
import store.server.config.ServerConfig;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.exception.SelfTrackingException;
import store.server.exception.UnreachableNodeException;
import store.server.providers.JsonBodyReader;
import store.server.storage.Procedure;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Manages nodes in the cluster.
 */
public class NodesService {

    private static final Logger LOG = LoggerFactory.getLogger(NodesService.class);

    private final Config config;
    private final StorageManager storageManager;
    private final NodesDao nodesDao;
    private final Guid guid;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param storageManager Persistent storage provider.
     * @param nodesDao Nodes definitions DAO.
     * @param attributesDao Attributes DAO.
     */
    public NodesService(Config config,
                        StorageManager storageManager,
                        NodesDao nodesDao,
                        final AttributesDao attributesDao) {

        this.config = config;
        this.storageManager = storageManager;
        this.nodesDao = nodesDao;
        this.guid = this.storageManager.inTransaction(new Query<Guid>() {
            @Override
            public Guid apply() {
                return attributesDao.guid();
            }
        });
    }

    /**
     * @return The definition of the local node.
     */
    public NodeDef getNodeDef() {
        return new NodeDef(name(), guid, uris(hosts()));
    }

    private String name() {
        if (!config.containsKey(ServerConfig.NODE_NAME)) {
            return defaultNodeName();
        }
        return config.getString(ServerConfig.NODE_NAME);
    }

    private static String defaultNodeName() {
        try {
            return InetAddress.getLocalHost().getHostName();

        } catch (UnknownHostException e) {
            LOG.warn("Failed to find node name", e);
            return "unknown";
        }
    }

    private List<String> hosts() {
        if (!config.containsKey(ServerConfig.NODE_PUBLISH_HOSTS)) {
            return defaultPublishHosts();
        }
        Value configVal = config.get(ServerConfig.NODE_PUBLISH_HOSTS);
        if (configVal.type() == ValueType.STRING) {
            return singletonList(configVal.asString());
        }
        return transform(configVal.asList(), new Function<Value, String>() {
            @Override
            public String apply(Value val) {
                return val.asString();
            }
        });
    }

    private List<String> defaultPublishHosts() {
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
            LOG.warn("Failed to find publish hosts", e);
        }
        sort(hosts);
        return hosts;
    }

    private List<URI> uris(List<String> hosts) {
        return transform(hosts, new Function<String, URI>() {
            @Override
            public URI apply(String input) {
                return UriBuilder.fromUri("http:/")
                        .host(input)
                        .port(config.getInt(ServerConfig.NODE_PORT))
                        .path(config.getString(ServerConfig.NODE_CONTEXT))
                        .build();
            }
        });
    }

    /**
     * Add a remote node to tracked ones. Fails if remote node is not reachable, is already tracked or its GUID is the
     * same as the local one.
     *
     * @param uris URIs of the remote node.
     */
    public void addRemote(List<URI> uris) {
        for (URI address : uris) {
            final Optional<NodeDef> def = downloadDef(address);
            if (def.isPresent()) {
                if (def.get().getGuid().equals(guid)) {
                    throw new SelfTrackingException();
                }
                storageManager.inTransaction(new Procedure() {
                    @Override
                    public void apply() {
                        nodesDao.createNodeDef(def.get());
                    }
                });
                return;
            }
        }
        throw new UnreachableNodeException();
    }

    private static Optional<NodeDef> downloadDef(URI uri) {
        ClientConfig clientConfig = new ClientConfig(JsonBodyReader.class);
        Client client = ClientBuilder.newClient(clientConfig);
        try {
            JsonObject json = client.target(uri)
                    .request()
                    .get()
                    .readEntity(JsonObject.class);

            return Optional.of(read(json, NodeDef.class));

        } catch (ProcessingException e) {
            return Optional.absent();

        } finally {
            client.close();
        }
    }

    /**
     * Stops tracking a remote node.
     *
     * @param key Node name or encoded GUID.
     */
    public void removeRemote(final String key) {
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                nodesDao.deleteNodeDef(key);
            }
        });
    }

    /**
     * Loads all remote nodes definitions.
     *
     * @return A list of NodeDef instances.
     */
    public List<NodeDef> listRemotes() {
        return storageManager.inTransaction(new Query<List<NodeDef>>() {
            @Override
            public List<NodeDef> apply() {
                return nodesDao.listNodeDefs();
            }
        });
    }
}
