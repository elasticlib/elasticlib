package store.common.json;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import javax.json.Json;
import javax.json.JsonObject;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import store.common.ReplicationDef;
import store.common.RepositoryDef;
import store.common.json.schema.Schema;

final class SchemaProvider {

    private static final Map<Class<?>, Schema> SCHEMAS = new HashMap<>();

    static {
        register(CommandResult.class,
                 ContentInfo.class,
                 ContentInfoTree.class,
                 Event.class,
                 IndexEntry.class,
                 RepositoryDef.class,
                 ReplicationDef.class);
    }

    private SchemaProvider() {
    }

    private static void register(Class<?>... classes) {
        for (Class<?> clazz : classes) {
            SCHEMAS.put(clazz, Schema.read(readJson(clazz.getSimpleName() + ".json")));
        }
    }

    private static JsonObject readJson(String filename) {
        Class<?> clazz = SchemaProvider.class;
        String resource = clazz.getPackage().getName().replace(".", "/") + "/" + filename;
        try (InputStream inputStream = clazz.getClassLoader().getResourceAsStream(resource);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                javax.json.JsonReader jsonReader = Json.createReader(reader)) {

            return jsonReader.readObject();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static Schema getSchema(Class<?> clazz) {
        return requireNonNull(SCHEMAS.get(clazz));
    }
}
