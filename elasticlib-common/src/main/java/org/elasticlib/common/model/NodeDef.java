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
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

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
            List<Value> values = publishUris.stream()
                    .map(host -> Value.of(host.toString()))
                    .collect(toList());

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
            return values.get(PUBLISH_URIS)
                    .asList()
                    .stream()
                    .map(value -> asUri(value))
                    .collect(toList());
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
