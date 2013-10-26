package store.common;

import com.google.common.base.Optional;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import static store.common.ContentInfo.contentInfo;

public final class JsonUtil {

    private JsonUtil() {
    }

    public static JsonArray writeContentInfos(List<ContentInfo> contentInfos) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (ContentInfo contentInfo : contentInfos) {
            builder.add(writeContentInfo(contentInfo));
        }
        return builder.build();
    }

    public static List<ContentInfo> readContentInfos(JsonArray json) {
        List<ContentInfo> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readContentInfo(object));
        }
        return list;
    }

    public static JsonObject writeContentInfo(ContentInfo contentInfo) {
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

    public static JsonObject writeConfig(Config config) {
        return createObjectBuilder()
                .add("volumes", writeUidMap(config.getVolumes()))
                .add("indexes", writeUidMap(config.getIndexes()))
                .add("write", writeOptionalUid(config.getWrite()))
                .add("read", writeOptionalUid(config.getRead()))
                .add("search", writeOptionalUid(config.getSearch()))
                .add("sync", writeSync(config))
                .build();
    }

    private static JsonObjectBuilder writeUidMap(Map<Uid, Path> map) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Entry<Uid, Path> entry : map.entrySet()) {
            json.add(entry.getKey().encode(), entry.getValue().toString());
        }
        return json;
    }

    private static JsonArrayBuilder writeOptionalUid(Optional<Uid> primary) {
        if (primary.isPresent()) {
            return createArrayBuilder().add(primary.get().encode());
        }
        return createArrayBuilder();
    }

    private static JsonObjectBuilder writeSync(Config config) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Uid source : config.getVolumes().keySet()) {
            Set<Uid> destinations = config.getSync(source);
            if (!destinations.isEmpty()) {
                JsonArrayBuilder array = createArrayBuilder();
                for (Uid uid : destinations) {
                    array.add(uid.encode());
                }
                json.add(source.encode(), array);
            }
        }
        return json;
    }

    public static Config readConfig(JsonObject json) {
        return new Config(readUidMap(json.getJsonObject("volumes")),
                          readUidMap(json.getJsonObject("indexes")),
                          readOptionalUid(json.getJsonArray("write")),
                          readOptionalUid(json.getJsonArray("read")),
                          readOptionalUid(json.getJsonArray("search")),
                          readSync(json.getJsonObject("sync")));
    }

    private static Map<Uid, Path> readUidMap(JsonObject json) {
        Map<Uid, Path> map = new LinkedHashMap<>();
        for (String key : json.keySet()) {
            map.put(new Uid(key), Paths.get(json.getString(key)));
        }
        return map;
    }

    private static Optional<Uid> readOptionalUid(JsonArray array) {
        if (array.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(new Uid(array.getString(0)));
    }

    private static Map<Uid, Set<Uid>> readSync(JsonObject json) {
        Map<Uid, Set<Uid>> map = new LinkedHashMap<>();
        for (String key : json.keySet()) {
            map.put(new Uid(key), readUids(json.getJsonArray(key)));
        }
        return map;
    }

    private static Set<Uid> readUids(JsonArray array) {
        Set<Uid> uids = new LinkedHashSet<>();
        for (JsonString object : array.getValuesAs(JsonString.class)) {
            uids.add(new Uid(object.getString()));
        }
        return uids;
    }

    public static JsonArray writeEvents(List<Event> events) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (Event event : events) {
            builder.add(writeEvent(event));
        }
        return builder.build();
    }

    public static List<Event> readEvents(JsonArray json) {
        List<Event> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readEvent(object));
        }
        return list;
    }

    private static JsonObjectBuilder writeEvent(Event event) {
        return createObjectBuilder()
                .add("seq", event.getSeq())
                .add("hash", event.getHash().encode())
                .add("timestamp", event.getTimestamp().getTime())
                .add("operation", event.getOperation().name());
    }

    private static Event readEvent(JsonObject json) {
        return Event.event()
                .withSeq(json.getJsonNumber("seq").longValue())
                .withHash(new Hash(json.getString("hash")))
                .withTimestamp(new Date(json.getJsonNumber("timestamp").longValue()))
                .withOperation(Operation.valueOf(json.getString("operation")))
                .build();
    }
}
