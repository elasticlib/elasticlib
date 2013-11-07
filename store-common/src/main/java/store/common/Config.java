package store.common;

import com.google.common.base.Optional;
import static com.google.common.base.Optional.absent;
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
import java.util.Map.Entry;
import java.util.Set;

public final class Config {

    private final Map<String, Path> volumes;
    private final Map<String, Path> indexes;
    private Optional<String> write;
    private Optional<String> read;
    private Optional<String> search;
    private final Map<String, Set<String>> sync;

    public Config() {
        volumes = new LinkedHashMap<>();
        indexes = new LinkedHashMap<>();
        write = absent();
        read = absent();
        search = absent();
        sync = new LinkedHashMap<>();
    }

    public Config(List<Path> volumes,
                  List<Path> indexes,
                  Optional<String> write,
                  Optional<String> read,
                  Optional<String> search,
                  Map<String, Set<String>> sync) {
        this.volumes = asMap(volumes);
        this.indexes = asMap(indexes);
        this.write = write;
        this.read = read;
        this.search = search;
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
        if (volumes.isEmpty()) {
            write = Optional.of(name(path));
            read = Optional.of(name(path));
        }
        volumes.put(name(path), path);
    }

    public void removeVolume(String name) {
        if (write.isPresent() && write.get().equals(name)) {
            write = absent();
        }
        if (read.isPresent() && read.get().equals(name)) {
            read = absent();
        }
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
        if (read.isPresent() && read.get().equals(volumeName) && !search.isPresent()) {
            search = Optional.of(name);
        }
    }

    public void removeIndex(String name) {
        if (search.isPresent() && search.get().equals(name)) {
            search = absent();
        }
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

    public void setWrite(String name) {
        write = Optional.of(name);
    }

    public void unsetWrite() {
        write = absent();
    }

    public void setRead(String name) {
        read = Optional.of(name);
        Set<String> volumeIndexes = getIndexes(name);
        if (!search.isPresent() || !volumeIndexes.contains(search.get())) {
            if (volumeIndexes.isEmpty()) {
                search = absent();
            } else {
                search = Optional.of(volumeIndexes.iterator().next());
            }
        }
    }

    public void unsetRead() {
        read = absent();
        search = absent();
    }

    public void setSearch(String name) {
        search = Optional.of(name);
        String volumeName = volumeName(name);
        if (!read.isPresent() || !read.get().equals(volumeName)) {
            read = Optional.of(volumeName);
        }
    }

    private String volumeName(String indexName) {
        for (Entry<String, Set<String>> entry : sync.entrySet()) {
            if (entry.getValue().contains(indexName)) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException();
    }

    public void unsetSearch() {
        search = absent();
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

    public Optional<String> getWrite() {
        return write;
    }

    public Optional<String> getRead() {
        return read;
    }

    public Optional<String> getSearch() {
        return search;
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
