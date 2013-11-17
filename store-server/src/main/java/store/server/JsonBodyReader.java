package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonStructure;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;

/**
 * JSON body reader.
 */
@Consumes({"application/json", "text/json", "*/*"})
public class JsonBodyReader implements MessageBodyReader<JsonStructure> {

    private final JsonReaderFactory rf = Json.createReaderFactory(null);
    private static final String JSON = "json";
    private static final String PLUS_JSON = "+json";

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return JsonStructure.class.isAssignableFrom(aClass) && supportsMediaType(mediaType);
    }

    /**
     * @return true for all media types of the pattern *&#47;json and *&#47;*+json.
     */
    private static boolean supportsMediaType(final MediaType mediaType) {
        return mediaType.getSubtype().equals(JSON) || mediaType.getSubtype().endsWith(PLUS_JSON);
    }

    @Override
    public JsonStructure readFrom(Class<JsonStructure> jsonStructureClass,
                                  Type type, Annotation[] annotations,
                                  MediaType mediaType,
                                  MultivaluedMap<String, String> stringStringMultivaluedMap,
                                  InputStream inputStream) throws IOException, WebApplicationException {
        try (JsonReader reader = rf.createReader(inputStream)) {
            return reader.read();
        }
    }
}
