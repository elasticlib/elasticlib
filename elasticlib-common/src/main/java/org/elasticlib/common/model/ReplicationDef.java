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
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Defines a replication.
 */
public final class ReplicationDef implements Mappable {

    private static final String GUID = "guid";
    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";

    private final Guid guid;
    private final Guid source;
    private final Guid destination;

    /**
     * Constructor.
     *
     * @param guid Replication GUID.
     * @param source Source repository GUID.
     * @param destination Destination repository GUID.
     */
    public ReplicationDef(Guid guid, Guid source, Guid destination) {
        this.guid = requireNonNull(guid);
        this.source = requireNonNull(source);
        this.destination = requireNonNull(destination);
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

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(GUID, guid)
                .put(SOURCE, source)
                .put(DESTINATION, destination)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ReplicationDef fromMap(Map<String, Value> map) {
        return new ReplicationDef(map.get(GUID).asGuid(),
                                  map.get(SOURCE).asGuid(),
                                  map.get(DESTINATION).asGuid());
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(GUID, guid)
                .add(SOURCE, source)
                .add(DESTINATION, destination)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(guid, source, destination);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReplicationDef)) {
            return false;
        }
        ReplicationDef other = (ReplicationDef) obj;
        return new EqualsBuilder()
                .append(guid, other.guid)
                .append(source, other.source)
                .append(destination, other.destination)
                .build();
    }
}
