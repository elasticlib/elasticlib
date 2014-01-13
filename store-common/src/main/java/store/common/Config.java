package store.common;

import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Config {

    private final Map<String, Path> repositories;
    private final Map<String, Set<String>> sync;

    public Config() {
        repositories = new LinkedHashMap<>();
        sync = new LinkedHashMap<>();
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

    public List<Path> getRepositories() {
        return new ArrayList<>(repositories.values());
    }

    public Set<String> getSync(String source) {
        if (!sync.containsKey(source)) {
            return emptySet();
        }
        return unmodifiableSet(sync.get(source));
    }
}
