/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Aggregates various info about a replication. If replication is not started, only its source and destination
 * definitions will be available.
 */
public final class ReplicationInfo implements Mappable {

    private static final String GUID = "guid";
    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private static final String SOURCE_DEF = "sourceDef";
    private static final String DESTINATION_DEF = "destinationDef";
    private static final String AGENT_INFO = "agentInfo";
    private static final String STARTED = "started";
    private static final String AGENT = "agent";

    private final Guid guid;
    private final Guid source;
    private final Guid destination;
    private final RepositoryDef sourceDef;
    private final RepositoryDef destinationdef;
    private final AgentInfo agentInfo;

    private ReplicationInfo(ReplicationInfoBuilder builder) {
        this.guid = builder.guid;
        this.source = builder.source;
        this.destination = builder.destination;
        this.sourceDef = builder.sourceDef;
        this.destinationdef = builder.destinationdef;
        this.agentInfo = builder.agentInfo;
    }

    /**
     * @return If this replication is started.
     */
    public boolean isStarted() {
        return agentInfo != null;
    }

    /**
     * @return The replication GUID.
     */
    public Guid getGuid() {
        return guid;
    }

    /**
     * @return The source repository GUID.
     */
    public Guid getSource() {
        return source;
    }

    /**
     * @return The destination repository GUID.
     */
    public Guid getDestination() {
        return destination;
    }

    /**
     * @return The source repository definition.
     */
    public Optional<RepositoryDef> getSourceDef() {
        return Optional.ofNullable(sourceDef);
    }

    /**
     * @return The destination repository definition.
     */
    public Optional<RepositoryDef> getDestinationdef() {
        return Optional.ofNullable(destinationdef);
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
                .put(GUID, guid)
                .put(SOURCE, map(source, sourceDef))
                .put(DESTINATION, map(destination, destinationdef))
                .put(STARTED, isStarted());

        if (isStarted()) {
            builder.put(AGENT, agentInfo.toMap());
        }
        return builder.build();
    }

    private static Map<String, Value> map(Guid guid, RepositoryDef def) {
        if (def == null) {
            return new MapBuilder().put(GUID, guid).build();
        }
        return def.toMap();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ReplicationInfo fromMap(Map<String, Value> map) {
        Map<String, Value> sourceMap = map.get(SOURCE).asMap();
        Map<String, Value> destinationMap = map.get(DESTINATION).asMap();
        ReplicationInfoBuilder builder = new ReplicationInfoBuilder(map.get(GUID).asGuid(),
                                                                    sourceMap.get(GUID).asGuid(),
                                                                    destinationMap.get(GUID).asGuid());
        if (sourceMap.size() > 1) {
            builder.withSourceDef(RepositoryDef.fromMap(sourceMap));
        }
        if (destinationMap.size() > 1) {
            builder.withDestinationDef(RepositoryDef.fromMap(destinationMap));
        }
        if (map.get(STARTED).asBoolean()) {
            builder.withAgentInfo(AgentInfo.fromMap(map.get(AGENT).asMap()));
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(GUID, guid)
                .add(SOURCE, source)
                .add(DESTINATION, destination)
                .add(SOURCE_DEF, sourceDef)
                .add(DESTINATION_DEF, destinationdef)
                .add(AGENT_INFO, agentInfo)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(guid, source, destination, sourceDef, destinationdef, agentInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReplicationInfo)) {
            return false;
        }
        ReplicationInfo other = (ReplicationInfo) obj;
        return new EqualsBuilder()
                .append(guid, other.guid)
                .append(source, other.source)
                .append(destination, other.destination)
                .append(sourceDef, other.sourceDef)
                .append(destinationdef, other.destinationdef)
                .append(agentInfo, other.agentInfo)
                .build();
    }

    /**
     * Builder.
     */
    public static class ReplicationInfoBuilder {

        private final Guid guid;
        private final Guid source;
        private final Guid destination;
        private RepositoryDef sourceDef;
        private RepositoryDef destinationdef;
        private AgentInfo agentInfo;

        /**
         * Constructor.
         *
         * @param guid Replication GUID.
         * @param source Source repository GUID.
         * @param destination Destination repository GUID.
         */
        public ReplicationInfoBuilder(Guid guid, Guid source, Guid destination) {
            this.guid = requireNonNull(guid);
            this.source = requireNonNull(source);
            this.destination = requireNonNull(destination);
        }

        /**
         * Set source repository definition.
         *
         * @param def Source repository definition.
         * @return this.
         */
        public ReplicationInfoBuilder withSourceDef(RepositoryDef def) {
            sourceDef = def;
            return this;
        }

        /**
         * Set destination repository definition.
         *
         * @param def Destination repository definition.
         * @return this.
         */
        public ReplicationInfoBuilder withDestinationDef(RepositoryDef def) {
            destinationdef = def;
            return this;
        }

        /**
         * Set replication agent info.
         *
         * @param info Replication agent info.
         * @return this.
         */
        public ReplicationInfoBuilder withAgentInfo(AgentInfo info) {
            agentInfo = info;
            return this;
        }

        /**
         * Build.
         *
         * @return A new ReplicationInfo instance.
         */
        public ReplicationInfo build() {
            return new ReplicationInfo(this);
        }
    }
}
