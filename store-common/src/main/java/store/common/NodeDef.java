package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.hash.Guid;
import store.common.value.Value;

/**
 * Define a node in a cluster.
 */
public class NodeDef implements Mappable {

    private static final String NAME = "name";
    private static final String GUID = "guid";
    private static final String HOSTS = "hosts";
    private static final String HOST = "host";
    private final String name;
    private final Guid guid;
    private final List<String> hosts;

    /**
     * Constructor.
     *
     * @param name Node name.
     * @param guid Node GUID.
     * @param hosts Node hosts.
     */
    public NodeDef(String name, Guid guid, List<String> hosts) {
        this.name = requireNonNull(name);
        this.guid = requireNonNull(guid);
        this.hosts = requireNonNull(hosts);
    }

    /**
     * @return The node name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The node GUID.
     */
    public Guid getGuid() {
        return guid;
    }

    /**
     * Provides the publish hosts of this node, ie the public addresses of this node.
     *
     * @return The publish addresses of this node.
     */
    public List<String> getHosts() {
        return hosts;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(NAME, name)
                .put(GUID, guid);

        if (hosts.size() == 1) {
            builder.put(HOST, hosts.get(0));

        } else {
            List<Value> values = new ArrayList<>();
            for (String host : hosts) {
                values.add(Value.of(host));
            }
            builder.put(HOSTS, values);
        }
        return builder.build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static NodeDef fromMap(Map<String, Value> map) {
        return new NodeDef(map.get(NAME).asString(),
                           map.get(GUID).asGuid(),
                           hosts(map));
    }

    private static List<String> hosts(Map<String, Value> values) {
        if (values.containsKey(HOST)) {
            return singletonList(values.get(HOST).asString());

        } else {
            List<String> hosts = new ArrayList<>();
            for (Value value : values.get(HOSTS).asList()) {
                hosts.add(value.asString());
            }
            return hosts;
        }
    }

    @Override
    public int hashCode() {
        return hash(name, guid, hosts);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeDef)) {
            return false;
        }
        NodeDef other = (NodeDef) obj;
        return new EqualsBuilder()
                .append(name, other.name)
                .append(guid, other.guid)
                .append(hosts, other.hosts)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NAME, name)
                .add(GUID, guid)
                .add(HOSTS, hosts)
                .toString();
    }
}
