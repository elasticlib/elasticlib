package store.common.io;

import java.math.BigDecimal;
import static java.util.Collections.unmodifiableMap;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import static store.common.io.Decoding.decodeMap;
import store.common.value.Value;

/**
 * A binary decoder. Converts a JSON-like binary structure into a map.
 *
 * @see ObjectEncoder
 */
public class ObjectDecoder {

    private final Map<String, Value> map;

    /**
     * Constructor.
     *
     * @param bytes A JSON-like binary structure.
     */
    public ObjectDecoder(byte[] bytes) {
        map = unmodifiableMap(decodeMap(new ByteArrayReader(bytes), bytes.length));
    }

    /**
     * Returns <tt>true</tt> if decoded structure contains a mapping for the specified key.
     *
     * @param key key whose presence in decoded structure is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified key
     */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /**
     * Returns a sorted {@link Set} of the keys contained in decoded structure.
     *
     * @return A set.
     */
    public Set<String> keyset() {
        return map.keySet();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a byte array.
     *
     * @param key The key whose associated value is to be returned.
     * @return A byte array.
     */
    public byte[] getByteArray(String key) {
        return get(key).asByteArray();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a boolean.
     *
     * @param key The key whose associated value is to be returned.
     * @return A boolean.
     */
    public boolean getBoolean(String key) {
        return get(key).asBoolean();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a byte.
     *
     * @param key The key whose associated value is to be returned.
     * @return A byte.
     */
    public byte getByte(String key) {
        return get(key).asByte();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not an integer.
     *
     * @param key The key whose associated value is to be returned.
     * @return An integer.
     */
    public int getInt(String key) {
        return get(key).asInt();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a long.
     *
     * @param key The key whose associated value is to be returned.
     * @return A long.
     */
    public long getLong(String key) {
        return get(key).asLong();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a big decimal.
     *
     * @param key The key whose associated value is to be returned.
     * @return A big decimal.
     */
    public BigDecimal getBigDecimal(String key) {
        return get(key).asBigDecimal();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a string.
     *
     * @param key The key whose associated value is to be returned.
     * @return A string.
     */
    public String getString(String key) {
        return get(key).asString();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a date.
     *
     * @param key The key whose associated value is to be returned.
     * @return A date.
     */
    public Date getDate(String key) {
        return get(key).asDate();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a map.
     *
     * @param key The key whose associated value is to be returned.
     * @return A map.
     */
    public Map<String, Value> getMap(String key) {
        return get(key).asMap();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a list.
     *
     * @param key The key whose associated value is to be returned.
     * @return A list.
     */
    public List<Value> getList(String key) {
        return get(key).asList();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key.
     *
     * @param key The key whose associated value is to be returned.
     * @return A Value.
     */
    public Value get(String key) {
        if (!map.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return map.get(key);
    }
}
