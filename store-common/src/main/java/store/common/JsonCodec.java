package store.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import store.common.Config;
import store.common.Hash;
import store.common.ContentInfo;

public final class JsonCodec {

    private JsonCodec() {
    }

    public static JsonObject encode(ContentInfo contentInfo) {
        return Json.createObjectBuilder()
                .add("hash", contentInfo.getHash().encode())
                .add("length", contentInfo.getLength())
                .add("metadata", encodeMap(contentInfo.getMetadata()))
                .build();
    }

    public static ContentInfo decodeContentInfo(JsonObject json) {
        return ContentInfo.contentInfo()
                .withHash(new Hash(json.getString("hash")))
                .withLength(json.getJsonNumber("length")
                .longValue())
                .withMetadata(decodeMap(json.getJsonObject("metadata")))
                .build();
    }

    private static JsonObjectBuilder encodeMap(Map<String, Object> map) {
        JsonObjectBuilder json = Json.createObjectBuilder();
        for (Entry<String, Object> entry : map.entrySet()) {
            json.add(entry.getKey(), entry.getValue().toString());
        }
        return json;
    }

    private static Map<String, Object> decodeMap(JsonObject json) {
        Map<String, Object> map = new HashMap<>();
        for (String key : json.keySet()) {
            map.put(key, json.getString(key));
        }
        return map;
    }

    public static JsonObject encode(Config config) {
        String root = config.getRoot().toAbsolutePath().toString();
        JsonArrayBuilder volumes = Json.createArrayBuilder();
        for (Path path : config.getVolumePaths()) {
            volumes.add(path.toAbsolutePath().toString());
        }
        return Json.createObjectBuilder()
                .add("root", root)
                .add("volumes", volumes)
                .build();
    }

    public static Config decodeConfig(JsonObject json) {
        Path root = Paths.get(json.getString("root"));
        List<Path> volumes = new ArrayList<>();
        for (JsonString value : json.getJsonArray("volumes").getValuesAs(JsonString.class)) {
            volumes.add(Paths.get(value.getString()));
        }
        return new Config(root, volumes);
    }
}
