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
package org.elasticlib.common.bson;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.elasticlib.common.bson.BinaryConstants.writeType;
import static org.elasticlib.common.bson.ValueWriting.writeKey;
import static org.elasticlib.common.bson.ValueWriting.writeValue;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.value.Value;

/**
 * A JSON-like binary writer with a fluent API.
 * <p>
 * Serialize a sequence of key-value pairs into a JSON-like binary document. Produced format supports the embedding of
 * documents and arrays within other documents and arrays. It also contains extensions that allow representation of data
 * types that are not part of the JSON spec.
 */
public final class BsonWriter {

    ByteArrayBuilder arrayBuilder = new ByteArrayBuilder();

    /**
     * Add a null value to the binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @return This encoder instance.
     */
    public BsonWriter putNull(String key) {
        put(key, Value.ofNull());
        return this;
    }

    /**
     * Add a hash to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, Hash value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a GUID to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, Guid value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a byte array to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, byte[] value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a boolean to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, boolean value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a long to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, long value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a string to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, String value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a big decimal to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, BigDecimal value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a date to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, Instant value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a map to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, Map<String, Value> value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a list to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, List<Value> value) {
        put(key, Value.of(value));
        return this;
    }

    /**
     * Add a value to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public BsonWriter put(String key, Value value) {
        arrayBuilder.append(writeType(value.type()))
                .append(writeKey(key))
                .append(writeValue(value));
        return this;
    }

    /**
     * Add a map of values to binary structure to build.
     *
     * @param map A map of values.
     * @return This encoder instance.
     */
    public BsonWriter put(Map<String, Value> map) {
        map.entrySet().forEach(entry -> {
            put(entry.getKey(), entry.getValue());
        });
        return this;
    }

    /**
     * Build the binary structure.
     *
     * @return Built structure as a byte array.
     */
    public byte[] build() {
        return arrayBuilder.build();
    }
}
