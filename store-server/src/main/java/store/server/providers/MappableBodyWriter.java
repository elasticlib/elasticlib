package store.server.providers;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.json.JsonWriter;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import store.common.json.JsonWriting;
import store.common.mappable.Mappable;
import store.common.yaml.YamlWriter;
import static store.server.providers.MessageBodyWriterUtil.isJson;
import static store.server.providers.MessageBodyWriterUtil.isYaml;
import static store.server.providers.MessageBodyWriterUtil.writerFactory;

/**
 * Custom HTTP body writer for Mappable instances. Produces either JSON or YAML. For JSON, nicely format output if
 * request contains query parameter "pretty=true".
 */
@Provider
@Produces({"application/json", "text/json", "application/yaml", "text/yaml", "*/*"})
public class MappableBodyWriter implements MessageBodyWriter<Mappable> {

    @Context
    private UriInfo uriInfo;

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (!Mappable.class.isAssignableFrom(type)) {
            return false;
        }
        return isJson(mediaType) || isYaml(mediaType);
    }

    @Override
    public long getSize(Mappable t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        // Deprecated by JAX-RS 2.0 and ignored by Jersey runtime.
        return -1;
    }

    @Override
    public void writeTo(Mappable t,
                        Class<?> type,
                        Type genericType,
                        Annotation[] annotations,
                        MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders,
                        OutputStream entityStream) throws IOException {

        if (isYaml(mediaType)) {
            writeYamlTo(t, entityStream);

        } else {
            writeJsonTo(t, entityStream);
        }
    }

    private void writeYamlTo(Mappable t, OutputStream entityStream) throws IOException {
        try (YamlWriter writer = new YamlWriter(entityStream)) {
            writer.write(t);
        }
    }

    private void writeJsonTo(Mappable t, OutputStream entityStream) {
        try (JsonWriter writer = writerFactory(uriInfo).createWriter(entityStream)) {
            writer.write(JsonWriting.write(t));
        }
    }
}
