package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static java.util.Objects.hash;
import java.util.Set;

public final class Config {

    private final Map<String, Path> repositories;
    private final Map<String, Set<String>> sync;

    public Config() {
        repositories = new LinkedHashMap<>();
        sync = new LinkedHashMap<>();
    }

    public Config(Config config) {
        this();
        for (Entry<String, Path> entry : config.repositories.entrySet()) {
            this.repositories.put(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Set<String>> entry : config.sync.entrySet()) {
            this.sync.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
    }

    public Config(List<Path> repositories, Map<String, Set<String>> sync) {
        this.repositories = new LinkedHashMap<>();
        for (Path path : repositories) {
            this.repositories.put(name(path), path);
        }
        this.sync = sync;
    }

    private static String name(Path path) {
        return path.getFileName().toString();
    }

    public void addRepository(Path path) {
        repositories.put(name(path), path);
    }

    public void removeRepository(String name) {
        unsync(name);
        repositories.remove(name);
    }

    private void unsync(String name) {
        if (sync.containsKey(name)) {
            sync.remove(name);
        }
        for (Set<String> to : sync.values()) {
            to.remove(name);
        }
    }

    public boolean sync(String source, String destination) {
        if (!sync.containsKey(source)) {
            sync.put(source, new LinkedHashSet<String>());
        }
        return sync.get(source).add(destination);
    }

    public boolean unsync(String source, String destination) {
        if (!sync.containsKey(source)) {
            return false;
        }
        boolean removed = sync.get(source).remove(destination);
        if (sync.get(source).isEmpty()) {
            sync.remove(source);
        }
        return removed;
    }

    public List<Path> getRepositories() {
        return new ArrayList<>(repositories.values());
    }

    public Set<String> getSync(String source) {
        if (!sync.containsKey(source)) {
            return emptySet();
        }
        return unmodifiableSet(sync.get(source));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("repositories", repositories)
                .add("sync", sync)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(repositories, sync);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Config)) {
            return false;
        }
        Config other = (Config) obj;
        return new EqualsBuilder()
                .append(repositories, other.repositories)
                .append(sync, other.sync)
                .build();
    }
}
