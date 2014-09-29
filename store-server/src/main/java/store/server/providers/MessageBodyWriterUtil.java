package store.server.providers;

import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

/**
 * Message body writers utilities.
 */
final class MessageBodyWriterUtil {

    private static final String YAML = "yaml";
    private static final String PLUS_YAML = "+yaml";
    private static final String JSON = "json";
    private static final String PLUS_JSON = "+json";
    private static final String PRETTY = "pretty";

    private MessageBodyWriterUtil() {
    }

    /**
     * Checks if supplied media type represents JSON.
     *
     * @param mediaType A MediaType instance.
     * @return true if this media type represents JSON.
     */
    public static boolean isJson(MediaType mediaType) {
        return mediaType.getSubtype().equals(JSON) || mediaType.getSubtype().endsWith(PLUS_JSON);
    }

    /**
     * Checks if supplied media type represents YAML.
     *
     * @param mediaType A MediaType instance.
     * @return true if this media type represents YAML.
     */
    public static boolean isYaml(MediaType mediaType) {
        return mediaType.getSubtype().equals(YAML) || mediaType.getSubtype().endsWith(PLUS_YAML);
    }

    /**
     * Creates a JsonWriterFactory.
     *
     * @param uriInfo Current request URI info.
     * @return A new JsonWriterFactory instance.
     */
    public static JsonWriterFactory writerFactory(UriInfo uriInfo) {
        Map<String, Object> properties = new HashMap<>();

        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        if (params.containsKey(PRETTY) && params.getFirst(PRETTY).equalsIgnoreCase("true")) {
            properties.put(JsonGenerator.PRETTY_PRINTING, true);
        }
        return Json.createWriterFactory(properties);
    }
}
