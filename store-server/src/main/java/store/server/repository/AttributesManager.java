package store.server.repository;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.nio.file.Files;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import java.nio.file.Path;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import store.common.hash.Guid;
import store.common.mappable.MapBuilder;
import store.common.value.Value;
import static store.common.value.Value.of;
import store.common.yaml.YamlReading;
import store.common.yaml.YamlWriting;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.WriteException;

/**
 * Manages immutable persisted attributes of a repository, that are currently its name and GUID.
 */
class AttributesManager {

    private static final String ATTRIBUTES = "attributes.yml";
    private static final String NAME = "name";
    private static final String GUID = "guid";
    private final String name;
    private final Guid guid;

    private AttributesManager(Map<String, Value> attributes) {
        this.name = requireNonNull(attributes.get(NAME).asString());
        this.guid = requireNonNull(attributes.get(GUID).asGuid());
    }

    public static AttributesManager create(Path path) {
        Map<String, Value> attributes = new MapBuilder()
                .put(NAME, path.getFileName().toString())
                .put(GUID, Guid.random())
                .build();
        try {
            write(path.resolve(ATTRIBUTES),
                  YamlWriting.writeValue(of(attributes)).getBytes(Charsets.UTF_8));

        } catch (IOException e) {
            throw new WriteException(e);
        }
        return new AttributesManager(attributes);
    }

    public static AttributesManager open(Path path) {
        if (!Files.exists(path.resolve(ATTRIBUTES))) {
            throw new InvalidRepositoryPathException();
        }
        try {
            String yaml = new String(readAllBytes(path.resolve(ATTRIBUTES)), Charsets.UTF_8);
            return new AttributesManager(YamlReading.readValue(yaml).asMap());

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    public String getName() {
        return name;
    }

    public Guid getGuid() {
        return guid;
    }
}
