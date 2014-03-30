package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import store.common.value.Value;

public final class Config implements Mappable {

    private static final String REPOSITORIES = "repositories";
    private static final String REPLICATIONS = "replications";
    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private final Map<String, Path> repositories;
    private final Map<String, Set<String>> replications;

    public Config() {
        repositories = new LinkedHashMap<>();
        replications = new LinkedHashMap<>();
    }

    public Config(Config config) {
        this();
        for (Entry<String, Path> entry : config.repositories.entrySet()) {
            this.repositories.put(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Set<String>> entry : config.replications.entrySet()) {
            this.replications.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
    }

    public Config(List<Path> repositories, Map<String, Set<String>> sync) {
        this.repositories = new LinkedHashMap<>();
        for (Path path : repositories) {
            this.repositories.put(name(path), path);
        }
        this.replications = sync;
    }

    private static String name(Path path) {
        return path.getFileName().toString();
    }

    public void addRepository(Path path) {
        repositories.put(name(path), path);
    }

    public void removeRepository(String name) {
        removeReplications(name);
        repositories.remove(name);
    }

    private void removeReplications(String name) {
        if (replications.containsKey(name)) {
            replications.remove(name);
        }
        for (Set<String> to : replications.values()) {
            to.remove(name);
        }
    }

    public boolean addReplication(String source, String destination) {
        if (!replications.containsKey(source)) {
            replications.put(source, new LinkedHashSet<String>());
        }
        return replications.get(source).add(destination);
    }

    public boolean removeReplication(String source, String destination) {
        if (!replications.containsKey(source)) {
            return false;
        }
        boolean removed = replications.get(source).remove(destination);
        if (replications.get(source).isEmpty()) {
            replications.remove(source);
        }
        return removed;
    }

    public List<Path> getRepositories() {
        return new ArrayList<>(repositories.values());
    }

    public Set<String> getReplications(String source) {
        if (!replications.containsKey(source)) {
            return emptySet();
        }
        return unmodifiableSet(replications.get(source));
    }

    @Override
    public Map<String, Value> toMap() {
        List<Value> repositoriesValue = new ArrayList<>();
        for (Path path : repositories.values()) {
            repositoriesValue.add(Value.of(path.toString()));
        }
        List<Value> replicationsValue = new ArrayList<>();
        for (Entry<String, Set<String>> entry : replications.entrySet()) {
            String source = entry.getKey();
            for (String destination : entry.getValue()) {
                replicationsValue.add(Value.of(new MapBuilder()
                        .put(SOURCE, source)
                        .put(DESTINATION, destination)
                        .build()));
            }
        }
        return new MapBuilder()
                .put(REPOSITORIES, repositoriesValue)
                .put(REPLICATIONS, replicationsValue)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static Config fromMap(Map<String, Value> map) {
        Config config = new Config();
        for (Value value : map.get(REPOSITORIES).asList()) {
            config.addRepository(Paths.get(value.asString()));
        }
        for (Value value : map.get(REPLICATIONS).asList()) {
            Map<String, Value> replication = value.asMap();
            config.addReplication(replication.get(SOURCE).asString(),
                                  replication.get(DESTINATION).asString());
        }
        return config;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(REPOSITORIES, repositories)
                .add(REPLICATIONS, replications)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(repositories, replications);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Config)) {
            return false;
        }
        Config other = (Config) obj;
        return new EqualsBuilder()
                .append(repositories, other.repositories)
                .append(replications, other.replications)
                .build();
    }
}
