package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.net.URI;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
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
    private static final String URIS = "uris";
    private static final String URI = "uri";
    private final String name;
    private final Guid guid;
    private final List<URI> uris;

    /**
     * Constructor.
     *
     * @param name Node name.
     * @param guid Node GUID.
     * @param uris Node URI(s).
     */
    public NodeDef(String name, Guid guid, List<URI> uris) {
        this.name = requireNonNull(name);
        this.guid = requireNonNull(guid);
        this.uris = requireNonNull(uris);
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
     * @return The base-URI(s) of this node.
     */
    public List<URI> getUris() {
        return uris;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(NAME, name)
                .put(GUID, guid);

        if (uris.size() == 1) {
            builder.put(URI, uris.get(0).toString());

        } else if (!uris.isEmpty()) {
            List<Value> values = new ArrayList<>();
            for (URI host : uris) {
                values.add(Value.of(host.toString()));
            }
            builder.put(URIS, values);
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

    private static List<URI> hosts(Map<String, Value> values) {
        if (values.containsKey(URI)) {
            return singletonList(asUri(values.get(URI)));

        } else if (values.containsKey(URIS)) {
            List<URI> hosts = new ArrayList<>();
            for (Value value : values.get(URIS).asList()) {
                hosts.add(asUri(value));
            }
            return hosts;

        } else {
            return emptyList();
        }
    }

    private static URI asUri(Value value) {
        return java.net.URI.create(value.asString());
    }

    @Override
    public int hashCode() {
        return hash(name, guid, uris);
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
                .append(uris, other.uris)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NAME, name)
                .add(GUID, guid)
                .add(URIS, uris)
                .toString();
    }
}
