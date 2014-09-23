package store.common.json;

import java.util.List;
import static javax.json.Json.createArrayBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import store.common.json.schema.Schema;
import store.common.mappable.Mappable;

/**
 * JSON writing utils.
 */
public final class JsonWriting {

    private JsonWriting() {
    }

    /**
     * Writes supplied {@link Mappable} to a JSON object.
     *
     * @param mappable A mappable instance.
     * @return A JSON object.
     */
    public static JsonObject write(Mappable mappable) {
        Schema schema = SchemaProvider.getSchema(mappable.getClass());
        return ValueWriting.writeMap(mappable.toMap(), schema).build();
    }

    /**
     * Writes supplied list of {@link Mappable} to a JSON array.
     *
     * @param mappables A list of mappables.
     * @return A JSON array.
     */
    public static JsonArray writeAll(List<? extends Mappable> mappables) {
        JsonArrayBuilder array = createArrayBuilder();
        for (Mappable mappable : mappables) {
            array.add(write(mappable));
        }
        return array.build();
    }
}
