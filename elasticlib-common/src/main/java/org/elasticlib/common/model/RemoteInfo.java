package org.elasticlib.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import java.net.URI;
import java.time.Instant;
import static java.time.Instant.now;
import java.util.List;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Info about a remote node from the point of view of the local one.
 */
public class RemoteInfo implements Mappable {

    private static final String NODE_INFO = "nodeInfo";
    private static final String REPOSITORIES = "repositories";
    private static final String TRANSPORT_URI = "transportUri";
    private static final String REACHABLE = "reachable";
    private static final String REFRESH_DATE = "refreshDate";
    private final NodeInfo nodeInfo;
    private final URI transportUri;
    private final Instant refreshDate;

    /**
     * Constructor for a reachable node.
     *
     * @param nodeInfo Node info.
     * @param transportUri Node transport URI.
     * @param refreshDate Refresh date of this info.
     */
    public RemoteInfo(NodeInfo nodeInfo, URI transportUri, Instant refreshDate) {
        this.nodeInfo = nodeInfo;
        this.transportUri = transportUri;
        this.refreshDate = refreshDate;
    }

    /**
     * Constructor for an unreachable node.
     *
     * @param nodeInfo Node info.
     * @param refreshDate Refresh date of this info.
     */
    public RemoteInfo(NodeInfo nodeInfo, Instant refreshDate) {
        this(nodeInfo, null, refreshDate);
    }

    /**
     * Provides a updated RemoteInfo instance by marking this one as unreachable.
     *
     * @return A new RemoteInfo instance.
     */
    public RemoteInfo asUnreachable() {
        return new RemoteInfo(nodeInfo, now());
    }

    /**
     * @return If this node is reachable.
     */
    public boolean isReachable() {
        return transportUri != null;
    }

    /**
     * @return The node definition.
     */
    public NodeDef getDef() {
        return nodeInfo.getDef();
    }

    /**
     * @return Info about the node repositories.
     */
    public List<RepositoryInfo> listRepositoryInfos() {
        return nodeInfo.listRepositoryInfos();
    }

    /**
     * @return The node transport URI. Fails if this node is not reachable.
     */
    public URI getTransportUri() {
        if (!isReachable()) {
            throw new IllegalStateException();
        }
        return transportUri;
    }

    /**
     * @return The refresh date of this info.
     */
    public Instant getRefreshDate() {
        return refreshDate;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .putAll(nodeInfo.getDef().toMap());

        if (isReachable()) {
            builder.put(TRANSPORT_URI, transportUri.toString());
        }
        builder.put(REACHABLE, isReachable())
                .put(REFRESH_DATE, refreshDate);

        if (!nodeInfo.listRepositoryInfos().isEmpty()) {
            builder.put(REPOSITORIES, nodeInfo.listRepositoryInfos()
                        .stream()
                        .map(x -> Value.of(x.toMap()))
                        .collect(toList()));
        }
        return builder.build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static RemoteInfo fromMap(Map<String, Value> map) {
        NodeInfo nodeInfo = NodeInfo.fromMap(map);
        if (!map.get(REACHABLE).asBoolean()) {
            return new RemoteInfo(nodeInfo, map.get(REFRESH_DATE).asInstant());
        }
        return new RemoteInfo(nodeInfo,
                              URI.create(map.get(TRANSPORT_URI).asString()),
                              map.get(REFRESH_DATE).asInstant());
    }

    @Override
    public int hashCode() {
        return hash(nodeInfo, transportUri, refreshDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RemoteInfo)) {
            return false;
        }
        RemoteInfo other = (RemoteInfo) obj;
        return new EqualsBuilder()
                .append(nodeInfo, other.nodeInfo)
                .append(transportUri, other.transportUri)
                .append(refreshDate, other.refreshDate)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NODE_INFO, nodeInfo)
                .add(TRANSPORT_URI, transportUri)
                .add(REFRESH_DATE, refreshDate)
                .toString();
    }
}
