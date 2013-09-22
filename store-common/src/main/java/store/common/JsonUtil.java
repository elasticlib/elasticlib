package store.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import static store.common.ContentInfo.contentInfo;

public final class JsonUtil {

    private JsonUtil() {
    }

    public static JsonObject write(ContentInfo contentInfo) {
        return createObjectBuilder()
                .add("hash", contentInfo.getHash().encode())
                .add("length", contentInfo.getLength())
                .add("metadata", writeMap(contentInfo.getMetadata()))
                .build();
    }

    public static ContentInfo readContentInfo(JsonObject json) {
        return contentInfo()
                .withHash(new Hash(json.getString("hash")))
                .withLength(json.getJsonNumber("length").longValue())
                .withMetadata(readMap(json.getJsonObject("metadata")))
                .build();
    }

    private static JsonObjectBuilder writeMap(Map<String, Object> map) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Entry<String, Object> entry : map.entrySet()) {
            json.add(entry.getKey(), entry.getValue().toString());
        }
        return json;
    }

    private static Map<String, Object> readMap(JsonObject json) {
        Map<String, Object> map = new HashMap<>();
        for (String key : json.keySet()) {
            map.put(key, json.getString(key));
        }
        return map;
    }

    public static JsonObject write(Config config) {
        String root = config.getRoot().toAbsolutePath().toString();
        JsonArrayBuilder volumes = createArrayBuilder();
        for (Path path : config.getVolumePaths()) {
            volumes.add(path.toAbsolutePath().toString());
        }
        return createObjectBuilder()
                .add("root", root)
                .add("volumes", volumes)
                .build();
    }

    public static Config readConfig(JsonObject json) {
        Path root = Paths.get(json.getString("root"));
        List<Path> volumes = new ArrayList<>();
        for (JsonString value : json.getJsonArray("volumes").getValuesAs(JsonString.class)) {
            volumes.add(Paths.get(value.getString()));
        }
        return new Config(root, volumes);
    }
}
