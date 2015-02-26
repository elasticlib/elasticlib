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
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Info about a node.
 */
public class NodeInfo implements Mappable {

    private static final String DEF = "def";
    private static final String REPOSITORIES = "repositories";
    private final NodeDef nodeDef;
    private final List<RepositoryInfo> repositoryInfos;

    /**
     * Constructor.
     *
     * @param nodeDef Node definition.
     * @param repositoryInfos Info about node repositories.
     */
    public NodeInfo(NodeDef nodeDef, List<RepositoryInfo> repositoryInfos) {
        this.nodeDef = nodeDef;
        this.repositoryInfos = repositoryInfos;
    }

    /**
     * @return The node name.
     */
    public String getName() {
        return nodeDef.getName();
    }

    /**
     * @return The node GUID.
     */
    public Guid getGuid() {
        return nodeDef.getGuid();
    }

    /**
     * @return The publish URI(s) of this node.
     */
    public List<URI> getPublishUris() {
        return nodeDef.getPublishUris();
    }

    /**
     * @return Info about the node repositories.
     */
    public List<RepositoryInfo> listRepositoryInfos() {
        return repositoryInfos;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .putAll(nodeDef.toMap());

        if (!repositoryInfos.isEmpty()) {
            builder.put(REPOSITORIES, repositoryInfos.stream()
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
    public static NodeInfo fromMap(Map<String, Value> map) {
        NodeDef def = NodeDef.fromMap(map);
        if (!map.containsKey(REPOSITORIES)) {
            return new NodeInfo(def, emptyList());
        }
        return new NodeInfo(def, map.get(REPOSITORIES)
                            .asList()
                            .stream()
                            .map(x -> RepositoryInfo.fromMap(x.asMap()))
                            .collect(toList()));
    }

    @Override
    public int hashCode() {
        return hash(nodeDef, repositoryInfos);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeInfo)) {
            return false;
        }
        NodeInfo other = (NodeInfo) obj;
        return new EqualsBuilder()
                .append(nodeDef, other.nodeDef)
                .append(repositoryInfos, other.repositoryInfos)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(DEF, nodeDef)
                .add(REPOSITORIES, repositoryInfos)
                .toString();
    }
}
