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
 * Defines a repository.
 */
public final class RepositoryDef implements Mappable {

    private static final String NAME = "name";
    private static final String GUID = "guid";
    private static final String PATH = "path";
    private final String name;
    private final Guid guid;
    private final String path;

    /**
     * Constructor.
     *
     * @param name Repository name.
     * @param guid Repository GUID.
     * @param path Repository path.
     */
    public RepositoryDef(String name, Guid guid, String path) {
        this.name = requireNonNull(name);
        this.guid = requireNonNull(guid);
        this.path = requireNonNull(path);
    }

    /**
     * Provides the name of the repository.
     *
     * @return The repository name.
     */
    public String getName() {
        return name;
    }

    /**
     * Provides the GUID of the repository.
     *
     * @return The repository GUID.
     */
    public Guid getGuid() {
        return guid;
    }

    /**
     * Provides the path of the repository. For a local repository, It corresponds to its the path on the node
     * file-system. For a remote repository, It corresponds to its URI on this node, or defaults to an empty string if
     * the node is unreachable.
     *
     * @return The repository path.
     */
    public String getPath() {
        return path;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(NAME, name)
                .put(GUID, guid);

        if (!path.isEmpty()) {
            builder.put(PATH, path);
        }
        return builder.build();
    }

    /**
     * Reads a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static RepositoryDef fromMap(Map<String, Value> map) {
        return new RepositoryDef(map.get(NAME).asString(),
                                 map.get(GUID).asGuid(),
                                 map.containsKey(PATH) ? map.get(PATH).asString() : "");
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NAME, name)
                .add(GUID, guid)
                .add(PATH, path)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(name, guid, path);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RepositoryDef)) {
            return false;
        }
        RepositoryDef other = (RepositoryDef) obj;
        return new EqualsBuilder()
                .append(name, other.name)
                .append(guid, other.guid)
                .append(path, other.path)
                .build();
    }
}
