package store.server.service;

import com.google.common.base.Function;
import static com.google.common.collect.Lists.transform;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import static java.net.NetworkInterface.getNetworkInterfaces;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import static java.util.Collections.sort;
import java.util.Enumeration;
import java.util.List;
import javax.ws.rs.core.UriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.NodeDef;
import store.common.config.Config;
import store.common.hash.Guid;
import store.common.value.Value;
import store.common.value.ValueType;
import store.server.config.ServerConfig;
import static store.server.storage.DatabaseEntries.asGuid;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Provides informations about the local node.
 */
public class NodeService {

    private static final String NODE = "node";
    private static final String GUID = "guid";
    private static final Logger LOG = LoggerFactory.getLogger(NodeService.class);
    private final Config config;
    private final Guid guid;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param storageManager Persistent storage provider.
     */
    public NodeService(Config config, StorageManager storageManager) {
        this.config = config;
        guid = guid(storageManager);
    }

    private static Guid guid(final StorageManager storageManager) {
        final Database nodeDb = storageManager.openDatabase(NODE);
        return storageManager.inTransaction(new Query<Guid>() {
            @Override
            public Guid apply() {
                DatabaseEntry key = entry(GUID);
                DatabaseEntry value = new DatabaseEntry();
                OperationStatus status = nodeDb.get(storageManager.currentTransaction(), key, value, LockMode.RMW);
                if (status == OperationStatus.SUCCESS) {
                    return asGuid(value);
                }
                Guid newGuid = Guid.random();
                nodeDb.put(storageManager.currentTransaction(), key, entry(newGuid));
                return newGuid;
            }
        });
    }

    /**
     * @return The definition of the local node.
     */
    public NodeDef getNodeDef() {
        return new NodeDef(name(), guid, urls(hosts()));
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
            return java.util.Collections.singletonList(configVal.asString());
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

    private List<String> urls(List<String> hosts) {
        return transform(hosts, new Function<String, String>() {
            @Override
            public String apply(String input) {
                return UriBuilder.fromUri("http:/")
                        .host(input)
                        .port(config.getInt(ServerConfig.NODE_PORT))
                        .path(config.getString(ServerConfig.NODE_CONTEXT))
                        .build()
                        .toString();
            }
        });
    }
}
