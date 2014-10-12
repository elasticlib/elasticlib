package store.common.json;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import store.common.exception.NodeException;
import store.common.json.schema.Schema;
import store.common.model.AgentInfo;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.common.model.ReplicationDef;
import store.common.model.ReplicationInfo;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import store.common.model.RepositoryStats;
import store.common.model.Revision;
import store.common.model.RevisionTree;
import store.common.model.StagingInfo;

/**
 * Provides schemas used to write/read/validate Mappable instances as JSON.
 */
final class SchemaProvider {

    private static final Map<Class<?>, Schema> SCHEMAS = new HashMap<>();

    static {
        register(CommandResult.class,
                 StagingInfo.class,
                 ContentInfo.class,
                 Revision.class,
                 RevisionTree.class,
                 Event.class,
                 IndexEntry.class,
                 RepositoryDef.class,
                 ReplicationDef.class,
                 AgentInfo.class,
                 RepositoryStats.class,
                 RepositoryInfo.class,
                 ReplicationInfo.class,
                 NodeDef.class,
                 NodeInfo.class,
                 NodeException.class);
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

    /**
     * Provides the schema associated with supplied class, or one of its parents classes.
     *
     * @param clazz A class.
     * @return Associated schema.
     */
    public static Schema getSchema(Class<?> clazz) {
        Schema schema = SCHEMAS.get(clazz);
        if (schema != null) {
            return schema;
        }
        Class<?> superClazz = clazz.getSuperclass();
        if (superClazz != null) {
            return getSchema(superClazz);
        }
        throw new AssertionError("No defined schema for " + clazz);
    }
}
