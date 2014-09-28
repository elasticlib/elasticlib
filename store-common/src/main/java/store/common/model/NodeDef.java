package store.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import java.net.URI;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.hash.Guid;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Define a node in a cluster.
 */
public class NodeDef implements Mappable {

    private static final String NAME = "name";
    private static final String GUID = "guid";
    private static final String PUBLISH_URIS = "publishUris";
    private static final String PUBLISH_URI = "publishUri";
    private final String name;
    private final Guid guid;
    private final List<URI> publishUris;

    /**
     * Constructor.
     *
     * @param name Node name.
     * @param guid Node GUID.
     * @param publishUris Node publish URI(s).
     */
    public NodeDef(String name, Guid guid, List<URI> publishUris) {
        this.name = requireNonNull(name);
        this.guid = requireNonNull(guid);
        this.publishUris = requireNonNull(publishUris);
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
     * @return The publish URI(s) of this node.
     */
    public List<URI> getPublishUris() {
        return publishUris;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(NAME, name)
                .put(GUID, guid);

        if (publishUris.size() == 1) {
            builder.put(PUBLISH_URI, publishUris.get(0).toString());

        } else if (!publishUris.isEmpty()) {
            List<Value> values = new ArrayList<>();
            for (URI host : publishUris) {
                values.add(Value.of(host.toString()));
            }
            builder.put(PUBLISH_URIS, values);
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
                           publishUris(map));
    }

    private static List<URI> publishUris(Map<String, Value> values) {
        if (values.containsKey(PUBLISH_URI)) {
            return singletonList(asUri(values.get(PUBLISH_URI)));
        }
        if (values.containsKey(PUBLISH_URIS)) {
            List<URI> uris = new ArrayList<>();
            for (Value value : values.get(PUBLISH_URIS).asList()) {
                uris.add(asUri(value));
            }
            return uris;
        }
        return emptyList();
    }

    private static URI asUri(Value value) {
        return java.net.URI.create(value.asString());
    }

    @Override
    public int hashCode() {
        return hash(name, guid, publishUris);
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
                .append(publishUris, other.publishUris)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NAME, name)
                .add(GUID, guid)
                .add(PUBLISH_URIS, publishUris)
                .toString();
    }
}
