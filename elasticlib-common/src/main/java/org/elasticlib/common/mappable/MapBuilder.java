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
package org.elasticlib.common.mappable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.value.Value;

/**
 * Support class for building maps of values.
 */
public class MapBuilder {

    private final Map<String, Value> map = new LinkedHashMap<>();

    /**
     * Put a hash entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Hash value) {
        return put(key, Value.of(value));
    }

    /**
     * Put a Guid entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Guid value) {
        return put(key, Value.of(value));
    }

    /**
     * Put a byte array entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, byte[] value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a boolean entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, boolean value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a long entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, long value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a string entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, String value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a big decimal entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, BigDecimal value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put an instant entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Instant value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a value entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Value value) {
        map.put(key, value);
        return this;
    }

    /**
     * Put a list entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, List<Value> value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a map entry in map to build.
     *
     * @param key Entry key.
     * @param value Corresponding value.
     * @return This builder.
     */
    public MapBuilder put(String key, Map<String, Value> value) {
        map.put(key, Value.of(value));
        return this;
    }

    /**
     * Put a set of entries in map to build.
     *
     * @param entries Entries to put.
     * @return This builder.
     */
    public MapBuilder putAll(Map<String, Value> entries) {
        entries.entrySet().forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return this;
    }

    /**
     * Build map.
     *
     * @return A map of values.
     */
    public Map<String, Value> build() {
        return map;
    }
}
