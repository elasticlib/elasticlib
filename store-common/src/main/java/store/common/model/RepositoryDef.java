package store.common.model;

import static com.google.common.base.Objects.toStringHelper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import store.common.hash.Guid;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Defines a repository.
 */
public final class RepositoryDef implements Mappable {

    private static final String NAME = "name";
    private static final String GUID = "guid";
    private static final String PATH = "path";
    private final String name;
    private final Guid guid;
    private final Path path;

    /**
     * Constructor.
     *
     * @param name Repository name.
     * @param guid Repository GUID.
     * @param path Repository path.
     */
    public RepositoryDef(String name, Guid guid, Path path) {
        this.name = requireNonNull(name);
        this.guid = requireNonNull(guid);
        this.path = requireNonNull(path);
    }

    /**
     * @return The repository name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The repository GUID.
     */
    public Guid getGuid() {
        return guid;
    }

    /**
     * @return The repository path.
     */
    public Path getPath() {
        return path;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(NAME, name)
                .put(GUID, guid)
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
                                 map.get(GUID).asGuid(),
                                 Paths.get(map.get(PATH).asString()));
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(NAME, name)
                .add(GUID, guid)
                .add(PATH, path)
                .toString();
    }

    @Override
    public int hashCode() {
        return hash(name, guid, path);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RepositoryDef)) {
            return false;
        }
        RepositoryDef other = (RepositoryDef) obj;
        return new EqualsBuilder()
                .append(name, other.name)
                .append(guid, other.guid)
                .append(path, other.path)
                .build();
    }
}