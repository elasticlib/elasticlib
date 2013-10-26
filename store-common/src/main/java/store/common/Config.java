package store.common;

import com.google.common.base.Optional;
import static com.google.common.base.Optional.absent;
import static com.google.common.collect.Sets.intersection;
import java.nio.file.Path;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class Config {

    private final Map<Uid, Path> volumes;
    private final Map<Uid, Path> indexes;
    private Optional<Uid> write;
    private Optional<Uid> read;
    private Optional<Uid> search;
    private final Map<Uid, Set<Uid>> sync;

    public Config() {
        volumes = new LinkedHashMap<>();
        indexes = new LinkedHashMap<>();
        write = absent();
        read = absent();
        search = absent();
        sync = new LinkedHashMap<>();
    }

    public Config(Map<Uid, Path> volumes,
                  Map<Uid, Path> indexes,
                  Optional<Uid> write,
                  Optional<Uid> read,
                  Optional<Uid> search,
                  Map<Uid, Set<Uid>> sync) {
        this.volumes = volumes;
        this.indexes = indexes;
        this.write = write;
        this.read = read;
        this.search = search;
        this.sync = sync;
    }

    public void addVolume(Uid uid, Path path) {
        if (volumes.isEmpty()) {
            write = Optional.of(uid);
            read = Optional.of(uid);
        }
        volumes.put(uid, path);
    }

    public void removeVolume(Uid uid) {
        if (write.isPresent() && write.get().equals(uid)) {
            write = absent();
        }
        if (read.isPresent() && read.get().equals(uid)) {
            read = absent();
        }
        for (Uid indexId : getIndexes(uid)) {
            removeIndex(indexId);
        }
        unsync(uid);
        volumes.remove(uid);
    }

    public void addIndex(Uid uid, Path path, Uid volumeId) {
        indexes.put(uid, path);
        sync(volumeId, uid);
        if (read.isPresent() && read.get().equals(volumeId) && !search.isPresent()) {
            search = Optional.of(uid);
        }
    }

    public void removeIndex(Uid uid) {
        if (search.isPresent() && search.get().equals(uid)) {
            search = absent();
        }
        unsync(uid);
        indexes.remove(uid);
    }

    private void unsync(Uid uid) {
        if (sync.containsKey(uid)) {
            sync.remove(uid);
        }
        for (Set<Uid> to : sync.values()) {
            to.remove(uid);
        }
    }

    public void setWrite(Uid uid) {
        write = Optional.of(uid);
    }

    public void unsetWrite() {
        write = absent();
    }

    public void setRead(Uid uid) {
        read = Optional.of(uid);
        Set<Uid> volumeIndexes = getIndexes(uid);
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

    public void setSearch(Uid uid) {
        search = Optional.of(uid);
        Uid volumeId = volumeId(uid);
        if (!read.isPresent() || !read.get().equals(volumeId)) {
            read = Optional.of(volumeId);
        }
    }

    private Uid volumeId(Uid indexId) {
        for (Map.Entry<Uid, Set<Uid>> entry : sync.entrySet()) {
            if (entry.getValue().contains(indexId)) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException();
    }

    public void unsetSearch() {
        search = absent();
    }

    public void sync(Uid source, Uid destination) {
        if (!sync.containsKey(source)) {
            sync.put(source, new LinkedHashSet<Uid>());
        }
        sync.get(source).add(destination);
    }

    public void unsync(Uid source, Uid destination) {
        sync.get(source).remove(destination);
        if (sync.get(source).isEmpty()) {
            sync.remove(source);
        }
    }

    public Map<Uid, Path> getVolumes() {
        return unmodifiableMap(volumes);
    }

    public Map<Uid, Path> getIndexes() {
        return unmodifiableMap(indexes);
    }

    public Optional<Uid> getWrite() {
        return write;
    }

    public Optional<Uid> getRead() {
        return read;
    }

    public Optional<Uid> getSearch() {
        return search;
    }

    public Set<Uid> getSync(Uid source) {
        if (!sync.containsKey(source)) {
            return emptySet();
        }
        return unmodifiableSet(sync.get(source));
    }

    public Set<Uid> getIndexes(Uid volume) {
        // La copie prévient une ConcurrentModificationException si on utilise ce résultat pour modifier la config.
        Set<Uid> copy = new HashSet<>(intersection(getSync(volume), indexes.keySet()));
        return unmodifiableSet(copy);
    }
}
