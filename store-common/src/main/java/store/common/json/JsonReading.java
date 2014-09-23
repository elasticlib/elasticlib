package store.common.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonObject;
import store.common.json.schema.Schema;
import store.common.mappable.Mappable;
import store.common.mappable.MappableUtil;
import store.common.value.Value;

/**
 * JSON reading utils.
 */
public final class JsonReading {

    private JsonReading() {
    }

    /**
     * Reads a {@link Mappable} from supplied JSON object.
     *
     * @param <T> Actual class to read.
     * @param json A JSON object.
     * @param clazz Actual class to read.
     * @return A new instance of supplied class.
     */
    public static <T extends Mappable> T read(JsonObject json, Class<T> clazz) {
        Schema schema = SchemaProvider.getSchema(clazz);
        Map<String, Value> values = ValueReading.readMap(json, schema);
        return MappableUtil.fromMap(values, clazz);
    }

    /**
     * Reads a list of {@link Mappable} from supplied JSON array.
     *
     * @param <T> Actual class to read.
     * @param array A JSON array.
     * @param clazz Actual class to read.
     * @return A list of new instances of supplied class.
     */
    public static <T extends Mappable> List<T> readAll(JsonArray array, Class<T> clazz) {
        List<T> list = new ArrayList<>(array.size());
        for (JsonObject json : array.getValuesAs(JsonObject.class)) {
            list.add(read(json, clazz));
        }
        return list;
    }
}
