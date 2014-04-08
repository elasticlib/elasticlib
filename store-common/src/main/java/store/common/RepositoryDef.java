package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.value.Value;

/**
 * Defines a repository.
 */
public final class RepositoryDef implements Mappable {

    private static final String NAME = "name";
    private static final String PATH = "path";
    private final String name;
    private final Path path;

    /**
     * Constructor.
     *
     * @param name Repository name.
     * @param path Repository path.
     */
    public RepositoryDef(String name, Path path) {
        this.name = requireNonNull(name);
        this.path = requireNonNull(path);
    }

    /**
     *
     * @return The repository name.
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return The repository path.
     */
    public Path getPath() {
        return path;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(NAME, name)
                .put(PATH, path.toString())
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static RepositoryDef fromMap(Map<String, Value> map) {
        return new RepositoryDef(map.get(NAME).asString(),
                                 Paths.get(map.get(PATH).asString()));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NAME, name)
                .add(PATH, path)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(name, path);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RepositoryDef)) {
            return false;
        }
        RepositoryDef other = (RepositoryDef) obj;
        return new EqualsBuilder()
                .append(name, other.name)
                .append(path, other.path)
                .build();
    }
}
