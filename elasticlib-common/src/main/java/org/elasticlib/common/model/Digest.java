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
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Hash and length of a given content.
 */
public class Digest implements Mappable {

    private static final String HASH = "hash";
    private static final String LENGTH = "length";
    private final Hash hash;
    private final Long length;

    /**
     * Constructor.
     *
     * @param hash Computed hash.
     * @param length Computed length.
     */
    public Digest(Hash hash, Long length) {
        this.hash = hash;
        this.length = length;
    }

    /**
     * @return Computed hash.
     */
    public Hash getHash() {
        return hash;
    }

    /**
     * @return Computed length.
     */
    public Long getLength() {
        return length;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(HASH, hash)
                .put(LENGTH, length)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static Digest fromMap(Map<String, Value> map) {
        return new Digest(map.get(HASH).asHash(), map.get(LENGTH).asLong());
    }

    @Override
    public int hashCode() {
        return hash(hash, length);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Digest)) {
            return false;
        }
        Digest other = (Digest) obj;
        return new EqualsBuilder()
                .append(hash, other.hash)
                .append(length, other.length)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(HASH, hash)
                .add(LENGTH, length)
                .toString();
    }
}
