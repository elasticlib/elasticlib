package store.server.providers;

import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonStructure;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

/**
 * Custom JSON body writer. Nicely format output if request contains query parameter "pretty=true".
 */
@Provider
@Produces({"application/json", "text/json", "*/*"})
public class JsonBodyWriter implements MessageBodyWriter<JsonStructure> {

    private static final String JSON = "json";
    private static final String PLUS_JSON = "+json";
    private static final String PRETTY = "pretty";
    @Context
    private UriInfo uriInfo;

    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return JsonStructure.class.isAssignableFrom(aClass) && supportsMediaType(mediaType);
    }

    /**
     * @return true for all media types of the pattern *&#47;json and *&#47;*+json.
     */
    private static boolean supportsMediaType(final MediaType mediaType) {
        return mediaType.getSubtype().equals(JSON) || mediaType.getSubtype().endsWith(PLUS_JSON);
    }

    @Override
    public long getSize(JsonStructure json, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        // Deprecated by JAX-RS 2.0 and ignored by Jersey runtime.
        return -1;
    }

    @Override
    public void writeTo(JsonStructure jsonStructure,
                        Class<?> aClass,
                        Type type,
                        Annotation[] annotations,
                        MediaType mediaType,
                        MultivaluedMap<String, Object> stringObjectMultivaluedMap,
                        OutputStream outputStream) {

        try (JsonWriter writer = writerFactory().createWriter(outputStream)) {
            writer.write(jsonStructure);
        }
    }

    private JsonWriterFactory writerFactory() {
        Map<String, Object> properties = new HashMap<>();
        if (isPrettyPrintingRequested()) {
            properties.put(JsonGenerator.PRETTY_PRINTING, true);
        }
        return Json.createWriterFactory(properties);
    }

    private boolean isPrettyPrintingRequested() {
        if (uriInfo == null) {
            // For client-side usage.
            return false;
        }
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        return params.containsKey(PRETTY) && params.getFirst(PRETTY).equalsIgnoreCase("true");
    }
}
