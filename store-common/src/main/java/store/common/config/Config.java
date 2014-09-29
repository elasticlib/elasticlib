package store.common.config;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.Collections;
import static java.util.Collections.emptyList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.value.Value;
import store.common.value.ValueType;
import store.common.yaml.YamlWriter;

/**
 * Configuration holder, built atop a Value. Root config value is expected to be a map, but this is not necessary.
 * Getter and setter may be requested to traverse this map (and its sub-maps) by using dot-separated keys. Immutable :
 * setter always returns new instances.
 */
public class Config {

    private static final String SEPARATOR = ".";
    private final Value root;

    /**
     * Empty config constructor.
     */
    public Config() {
        this.root = Value.of(Collections.<String, Value>emptyMap());
    }

    /**
     * Constructor.
     *
     * @param root Root config value.
     */
    public Config(Value root) {
        this.root = requireNonNull(root);
    }

    /**
     * Override this config with supplied one.
     *
     * @param extension A config instance.
     * @return A new config instance.
     */
    public Config extend(Config extension) {
        return new Config(extend(root, extension.get("")));
    }

    private static Value extend(Value base, Value extension) {
        if (base.type() != ValueType.OBJECT || extension.type() != ValueType.OBJECT) {
            return extension;
        }
        Map<String, Value> baseMap = base.asMap();
        Map<String, Value> extensionMap = extension.asMap();
        Map<String, Value> result = new LinkedHashMap<>();
        for (Entry<String, Value> entry : baseMap.entrySet()) {
            if (extensionMap.containsKey(entry.getKey())) {
                result.put(entry.getKey(), extend(entry.getValue(), extensionMap.get(entry.getKey())));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        for (Entry<String, Value> entry : extensionMap.entrySet()) {
            if (!baseMap.containsKey(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return Value.of(result);
    }

    /**
     * Checks if this config is empty.
     *
     * @return true if this is the case.
     */
    public boolean isEmpty() {
        return root.type() == ValueType.OBJECT && root.asMap().isEmpty();
    }

    /**
     * Provides root value of this config.
     *
     * @return A value.
     */
    public Value asValue() {
        return root;
    }

    /**
     * List keys of this config.
     *
     * @return A list of keys.
     */
    public List<String> listKeys() {
        return listKeys("", root);
    }

    private List<String> listKeys(String path, Value node) {
        if (node.type() != ValueType.OBJECT) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>();
        for (Entry<String, Value> entry : node.asMap().entrySet()) {
            String key = path.isEmpty() ? entry.getKey() : key(path, entry.getKey());
            List<String> subKeys = listKeys(key, entry.getValue());
            if (subKeys.isEmpty()) {
                keys.add(key);
            } else {
                keys.addAll(subKeys);
            }
        }
        return keys;
    }

    /**
     * Provides value associated with supplied key. If an empty key is supplied, the root config value is returned.
     * Fails if this config or its default do not have any value associated with supplied key.
     *
     * @param key A key.
     * @return Corresponding value.
     */
    public Value get(String key) {
        Optional<Value> value = get(root, path(key));
        if (value.isPresent()) {
            return value.get();
        }
        throw new ConfigException("Undefined config key '" + key + "'");
    }

    /**
     * Checks if this config contains a value associated with supplied key.
     *
     * @param key A key.
     * @return true if corresponding value exists.
     */
    public boolean containsKey(String key) {
        return get(root, path(key)).isPresent();
    }

    /**
     * Provides a flat map of value associated with supplied key. Fails if this value is not a map. Here, flat map means
     * that value is traversed and all its sub-maps are unnested.
     *
     * @param key A key.
     * @return Corresponding value as a a flat map.
     */
    public Map<String, Value> getFlatMap(String key) {
        return flatMap(key, getWithType(key, ValueType.OBJECT).asMap());
    }

    private static Map<String, Value> flatMap(String key, Map<String, Value> map) {
        Map<String, Value> flatMap = new LinkedHashMap<>();
        for (Entry<String, Value> entry : map.entrySet()) {
            String flatKey = key(key, entry.getKey());
            Value value = entry.getValue();
            if (value.type() != ValueType.OBJECT) {
                flatMap.put(flatKey, value);
            } else {
                for (Entry<String, Value> subEntry : flatMap(flatKey, value.asMap()).entrySet()) {
                    flatMap.put(subEntry.getKey(), subEntry.getValue());
                }
            }
        }
        return flatMap;
    }

    /**
     * Convenience overload for getting a string value.
     *
     * @param key A key.
     * @return Corresponding value.
     */
    public String getString(String key) {
        return getWithType(key, ValueType.STRING).asString();
    }

    /**
     * Convenience overload for getting a boolean value.
     *
     * @param key A key.
     * @return Corresponding value.
     */
    public boolean getBoolean(String key) {
        return getWithType(key, ValueType.BOOLEAN).asBoolean();
    }

    /**
     * Convenience overload for getting a long value.
     *
     * @param key A key.
     * @return Corresponding value.
     */
    public long getLong(String key) {
        return getWithType(key, ValueType.INTEGER).asLong();
    }

    /**
     * Convenience overload for getting an int value.
     *
     * @param key A key.
     * @return Corresponding value.
     */
    public int getInt(String key) {
        return Ints.checkedCast(getLong(key));
    }

    private Value getWithType(String key, ValueType expected) {
        Value value = get(key);
        if (value.type() != expected) {
            throw new ConfigException(Joiner.on(" ").join("Key",
                                                          key,
                                                          "is expected to have an",
                                                          expected.toString().toLowerCase(),
                                                          "value"));
        }
        return value;
    }

    private static Optional<Value> get(Value tree, List<String> path) {
        if (path.isEmpty()) {
            return Optional.of(tree);
        }
        Optional<Value> node = node(tree, path.get(0));
        List<String> subPath = path.subList(1, path.size());
        if (subPath.isEmpty()) {
            return node;
        }
        if (!node.isPresent() || node.get().type() != ValueType.OBJECT) {
            return Optional.absent();
        }
        return get(node.get(), subPath);
    }

    private static Optional<Value> node(Value tree, String key) {
        if (tree.type() != ValueType.OBJECT || !tree.asMap().containsKey(key)) {
            return Optional.absent();
        }
        return Optional.of(tree.asMap().get(key));
    }

    /**
     * Associates supplied value with supplied key. If an empty key is supplied, the root config value is updated.
     *
     * @param key A key.
     * @param value A value.
     * @return A new config instance with updated value.
     */
    public Config set(String key, Value value) {
        return new Config(set(root, path(key), value));
    }

    /**
     * Convenience overload for setting a string value.
     *
     * @param key A key.
     * @param value A value.
     * @return A new config instance with updated value.
     */
    public Config set(String key, String value) {
        return set(key, Value.of(value));
    }

    /**
     * Convenience overload for setting a boolean value.
     *
     * @param key A key.
     * @param value A value.
     * @return A new config instance with updated value.
     */
    public Config set(String key, boolean value) {
        return set(key, Value.of(value));
    }

    /**
     * Convenience overload for setting a long value.
     *
     * @param key A key.
     * @param value A value.
     * @return A new config instance with updated value.
     */
    public Config set(String key, long value) {
        return set(key, Value.of(value));
    }

    private static Value set(Value tree, List<String> path, Value value) {
        if (path.isEmpty()) {
            return value;
        }
        List<String> head = path.subList(0, path.size() - 1);
        String tail = path.get(path.size() - 1);
        Optional<Value> parent = get(tree, head);
        if (!parent.isPresent() || parent.get().type() != ValueType.OBJECT) {
            return set(tree, head, Value.of(ImmutableMap.of(tail, value)));
        }
        Map<String, Value> parentTree = new LinkedHashMap<>(parent.get().asMap());
        parentTree.put(tail, value);
        return set(tree, head, Value.of(parentTree));
    }

    /**
     * Remove value associated with supplied key, if any. If an empty key is supplied, the root config value is cleared.
     *
     * @param key A key.
     * @return A new config instance without removed value.
     */
    public Config unset(String key) {
        return new Config(unset(root, path(key)));
    }

    private static Value unset(Value tree, List<String> path) {
        if (path.isEmpty()) {
            return Value.of(Collections.<String, Value>emptyMap());
        }
        List<String> head = path.subList(0, path.size() - 1);
        String tail = path.get(path.size() - 1);
        Optional<Value> parent = get(tree, head);
        if (!parent.isPresent() || parent.get().type() != ValueType.OBJECT) {
            return tree;
        }
        Map<String, Value> parentTree = parent.get().asMap();
        if (parentTree.size() == 1 && parentTree.containsKey(tail)) {
            return unset(tree, head);
        }
        parentTree = new LinkedHashMap<>(parentTree);
        parentTree.remove(tail);
        return set(tree, head, Value.of(parentTree));
    }

    private static List<String> path(String key) {
        if (key.isEmpty()) {
            return emptyList();
        }
        return Splitter.on(SEPARATOR).splitToList(key);
    }

    /**
     * Build config key by joining supplied path.
     *
     * @param path A path in config tree.
     * @return Corresponding key.
     */
    public static String key(String... path) {
        return Joiner.on(SEPARATOR).join(path);
    }

    @Override
    public int hashCode() {
        return hash(root);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Config)) {
            return false;
        }
        Config other = (Config) obj;
        return root.equals(other.root);
    }

    @Override
    public String toString() {
        return YamlWriter.writeToString(root);
    }
}
