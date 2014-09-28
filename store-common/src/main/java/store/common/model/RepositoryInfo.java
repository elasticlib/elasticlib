package store.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Aggregates various info about a repository. If repository is not open, only its definition will be available.
 */
public final class RepositoryInfo implements Mappable {

    private static final String OPEN = "open";
    private static final String DEF = "def";
    private static final String STATS = "stats";
    private static final String INDEXING_INFO = "indexationInfo";
    private static final String STATS_INFO = "statsInfo";
    private static final String AGENTS = "agents";
    private static final String INDEXING = "indexing";
    private final RepositoryDef def;
    private final RepositoryStats stats;
    private final AgentInfo indexingInfo;
    private final AgentInfo statsInfo;

    /**
     * Constructor for an open repository.
     *
     * @param def Repository definition.
     * @param stats Repository statistics.
     * @param indexingInfo Repository indexing agent info.
     * @param statsInfo Repository stats computing agent info.
     */
    public RepositoryInfo(RepositoryDef def, RepositoryStats stats, AgentInfo indexingInfo, AgentInfo statsInfo) {
        this.def = requireNonNull(def);
        this.stats = stats;
        this.indexingInfo = indexingInfo;
        this.statsInfo = statsInfo;
    }

    /**
     * Constructor for a closed repository.
     *
     * @param def Repository definition.
     */
    public RepositoryInfo(RepositoryDef def) {
        this(def, null, null, null);
    }

    /**
     * @return If this repository is open.
     */
    public boolean isOpen() {
        return stats != null;
    }

    /**
     * @return The definition of this repository.
     */
    public RepositoryDef getDef() {
        return def;
    }

    /**
     * @return Statistics of this repository. Fails if repository is not open.
     */
    public RepositoryStats getStats() {
        if (!isOpen()) {
            throw new IllegalStateException();
        }
        return stats;
    }

    /**
     * @return Info about the indexing agent of this repository. Fails if repository is not open.
     */
    public AgentInfo getIndexingInfo() {
        if (!isOpen()) {
            throw new IllegalStateException();
        }
        return indexingInfo;
    }

    /**
     * @return Info about the stats computing agent of this repository. Fails if repository is not open.
     */
    public AgentInfo getStatsInfo() {
        if (!isOpen()) {
            throw new IllegalStateException();
        }
        return statsInfo;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .putAll(def.toMap())
                .put(OPEN, isOpen());

        if (isOpen()) {
            builder.put(STATS, stats.toMap())
                    .put(AGENTS, ImmutableMap.of(INDEXING, Value.of(indexingInfo.toMap()),
                                                 STATS, Value.of(statsInfo.toMap())));
        }
        return builder.build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static RepositoryInfo fromMap(Map<String, Value> map) {
        RepositoryDef def = RepositoryDef.fromMap(map);
        if (!map.get(OPEN).asBoolean()) {
            return new RepositoryInfo(def);
        }
        Map<String, Value> agents = map.get(AGENTS).asMap();
        return new RepositoryInfo(def,
                                  RepositoryStats.fromMap(map.get(STATS).asMap()),
                                  AgentInfo.fromMap(agents.get(INDEXING).asMap()),
                                  AgentInfo.fromMap(agents.get(STATS).asMap()));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(DEF, def)
                .add(STATS, stats)
                .add(INDEXING_INFO, indexingInfo)
                .add(STATS_INFO, statsInfo)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(def, stats, indexingInfo, statsInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RepositoryInfo)) {
            return false;
        }
        RepositoryInfo other = (RepositoryInfo) obj;
        return new EqualsBuilder()
                .append(def, other.def)
                .append(stats, other.stats)
                .append(indexingInfo, other.indexingInfo)
                .append(statsInfo, other.statsInfo)
                .build();
    }
}
