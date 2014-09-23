package store.common.model;

import static com.google.common.base.Objects.toStringHelper;
import java.util.Map;
import static java.util.Objects.hash;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Aggregates various info about a replication. If replication is not started, only its source and destination
 * definitions will be available.
 */
public final class ReplicationInfo implements Mappable {

    private static final String SOURCE_DEF = "sourceDef";
    private static final String DESTINATION_DEF = "destinationdef";
    private static final String AGENT_INFO = "agentInfo";
    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private static final String STARTED = "started";
    private static final String AGENT = "agent";
    private final RepositoryDef sourceDef;
    private final RepositoryDef destinationdef;
    private final AgentInfo agentInfo;

    /**
     * Constructor for an started replication.
     *
     * @param sourceDef Source repository definition.
     * @param destinationdef Destination repository definition.
     * @param agentInfo Replication agent info.
     */
    public ReplicationInfo(RepositoryDef sourceDef, RepositoryDef destinationdef, AgentInfo agentInfo) {
        this.sourceDef = sourceDef;
        this.destinationdef = destinationdef;
        this.agentInfo = agentInfo;
    }

    /**
     * Constructor for a stopped replication.
     *
     * @param sourceDef Source repository definition.
     * @param destinationdef Destination repository definition.
     */
    public ReplicationInfo(RepositoryDef sourceDef, RepositoryDef destinationdef) {
        this(sourceDef, destinationdef, null);
    }

    /**
     * @return If this replication is started.
     */
    public boolean isStarted() {
        return agentInfo != null;
    }

    /**
     * @return The source repository definition.
     */
    public RepositoryDef getSourceDef() {
        return sourceDef;
    }

    /**
     * @return The destination repository definition.
     */
    public RepositoryDef getDestinationdef() {
        return destinationdef;
    }

    /**
     * @return The replication agent info. Fails if replication is not started.
     */
    public AgentInfo getAgentInfo() {
        if (!isStarted()) {
            throw new IllegalStateException();
        }
        return agentInfo;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(SOURCE, sourceDef.toMap())
                .put(DESTINATION, destinationdef.toMap())
                .put(STARTED, isStarted());

        if (isStarted()) {
            builder.put(AGENT, agentInfo.toMap());
        }
        return builder.build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ReplicationInfo fromMap(Map<String, Value> map) {
        if (!map.get(STARTED).asBoolean()) {
            return new ReplicationInfo(RepositoryDef.fromMap(map.get(SOURCE).asMap()),
                                       RepositoryDef.fromMap(map.get(DESTINATION).asMap()));
        }
        return new ReplicationInfo(RepositoryDef.fromMap(map.get(SOURCE).asMap()),
                                   RepositoryDef.fromMap(map.get(DESTINATION).asMap()),
                                   AgentInfo.fromMap(map.get(AGENT).asMap()));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(SOURCE_DEF, sourceDef)
                .add(DESTINATION_DEF, destinationdef)
                .add(AGENT_INFO, agentInfo)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(sourceDef, destinationdef, agentInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReplicationInfo)) {
            return false;
        }
        ReplicationInfo other = (ReplicationInfo) obj;
        return new EqualsBuilder()
                .append(sourceDef, other.sourceDef)
                .append(destinationdef, other.destinationdef)
                .append(agentInfo, other.agentInfo)
                .build();
    }
}
