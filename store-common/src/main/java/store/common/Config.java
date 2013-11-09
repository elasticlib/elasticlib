package store.common;

import static com.google.common.collect.Sets.intersection;
import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Config {

    private final Map<String, Path> volumes;
    private final Map<String, Path> indexes;
    private final Map<String, Set<String>> sync;

    public Config() {
        volumes = new LinkedHashMap<>();
        indexes = new LinkedHashMap<>();
        sync = new LinkedHashMap<>();
    }

    public Config(List<Path> volumes,
                  List<Path> indexes,
                  Map<String, Set<String>> sync) {
        this.volumes = asMap(volumes);
        this.indexes = asMap(indexes);
        this.sync = sync;
    }

    private static Map<String, Path> asMap(List<Path> list) {
        Map<String, Path> map = new LinkedHashMap<>();
        for (Path path : list) {
            map.put(name(path), path);
        }
        return map;
    }

    private static String name(Path path) {
        return path.getFileName().toString();
    }

    public void addVolume(Path path) {
        volumes.put(name(path), path);
    }

    public void removeVolume(String name) {
        for (String indexName : getIndexes(name)) {
            removeIndex(indexName);
        }
        unsync(name);
        volumes.remove(name);
    }

    public void addIndex(Path path, String volumeName) {
        String name = name(path);
        indexes.put(name, path);
        sync(volumeName, name);
    }

    public void removeIndex(String name) {
        unsync(name);
        indexes.remove(name);
    }

    private void unsync(String name) {
        if (sync.containsKey(name)) {
            sync.remove(name);
        }
        for (Set<String> to : sync.values()) {
            to.remove(name);
        }
    }

    public void sync(String source, String destination) {
        if (!sync.containsKey(source)) {
            sync.put(source, new LinkedHashSet<String>());
        }
        sync.get(source).add(destination);
    }

    public void unsync(String source, String destination) {
        sync.get(source).remove(destination);
        if (sync.get(source).isEmpty()) {
            sync.remove(source);
        }
    }

    public List<Path> getVolumes() {
        return new ArrayList<>(volumes.values());
    }

    public List<Path> getIndexes() {
        return new ArrayList<>(indexes.values());
    }

    public Set<String> getSync(String source) {
        if (!sync.containsKey(source)) {
            return emptySet();
        }
        return unmodifiableSet(sync.get(source));
    }

    public Set<String> getIndexes(String volume) {
        // La copie prévient une ConcurrentModificationException si on utilise ce résultat pour modifier la config.
        Set<String> copy = new HashSet<>(intersection(getSync(volume), indexes.keySet()));
        return unmodifiableSet(copy);
    }
}
