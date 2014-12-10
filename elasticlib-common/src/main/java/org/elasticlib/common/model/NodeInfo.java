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
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import static java.util.Objects.hash;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Info about a remote node from the point of view of the local one.
 */
public class NodeInfo implements Mappable {

    private static final String DEF = "def";
    private static final String TRANSPORT_URI = "transportUri";
    private static final String REACHABLE = "reachable";
    private static final String REFRESH_DATE = "refreshDate";
    private final NodeDef def;
    private final URI transportUri;
    private final Instant refreshDate;

    /**
     * Constructor for a reachable node.
     *
     * @param def Node definition
     * @param transportUri Node transport URI.
     * @param refreshDate Refresh date of this info.
     */
    public NodeInfo(NodeDef def, URI transportUri, Instant refreshDate) {
        this.def = def;
        this.transportUri = transportUri;
        this.refreshDate = refreshDate;
    }

    /**
     * Constructor for an unreachable node.
     *
     * @param def Node definition
     * @param refreshDate Refresh date of this info.
     */
    public NodeInfo(NodeDef def, Instant refreshDate) {
        this(def, null, refreshDate);
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
        return def;
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
                .putAll(def.toMap());

        if (isReachable()) {
            builder.put(TRANSPORT_URI, transportUri.toString());
        }

        return builder.put(REACHABLE, isReachable())
                .put(REFRESH_DATE, refreshDate)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static NodeInfo fromMap(Map<String, Value> map) {
        NodeDef def = NodeDef.fromMap(map);
        if (!map.get(REACHABLE).asBoolean()) {
            return new NodeInfo(def, map.get(REFRESH_DATE).asInstant());
        }
        return new NodeInfo(def,
                            URI.create(map.get(TRANSPORT_URI).asString()),
                            map.get(REFRESH_DATE).asInstant());
    }

    @Override
    public int hashCode() {
        return hash(def, transportUri, refreshDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeInfo)) {
            return false;
        }
        NodeInfo other = (NodeInfo) obj;
        return new EqualsBuilder()
                .append(def, other.def)
                .append(transportUri, other.transportUri)
                .append(refreshDate, other.refreshDate)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(DEF, def)
                .add(TRANSPORT_URI, transportUri)
                .add(REFRESH_DATE, refreshDate)
                .toString();
    }
}
