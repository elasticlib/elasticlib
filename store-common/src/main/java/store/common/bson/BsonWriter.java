package store.common.bson;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static store.common.bson.BinaryConstants.writeType;
import static store.common.bson.ValueWriting.writeKey;
import static store.common.bson.ValueWriting.writeValue;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.value.Value;

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
