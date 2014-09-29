package store.server.providers;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
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
 * Custom HTTP body writer for list of Mappable instances. Produces either JSON or YAML. For JSON, nicely format output
 * if request contains query parameter "pretty=true".
 */
@Provider
@Produces({"application/json", "text/json", "application/yaml", "text/yaml", "*/*"})
public class MappableListBodyWriter implements MessageBodyWriter<List<? extends Mappable>> {

    @Context
    private UriInfo uriInfo;

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (!List.class.isAssignableFrom(type) || !containsMappables(genericType)) {
            return false;
        }
        return isJson(mediaType) || isYaml(mediaType);
    }

    private static boolean containsMappables(Type genericType) {
        if (!(genericType instanceof ParameterizedType)) {
            return false;
        }

        ParameterizedType pt = (ParameterizedType) genericType;
        if (pt.getActualTypeArguments().length == 0) {
            return true;
        }
        if (pt.getActualTypeArguments().length > 1) {
            return false;
        }

        Type itemType = pt.getActualTypeArguments()[0];
        if (!(itemType instanceof Class)) {
            return false;
        }
        return Mappable.class.isAssignableFrom(Class.class.cast(itemType));
    }

    @Override
    public long getSize(List<? extends Mappable> t,
                        Class<?> type,
                        Type genericType,
                        Annotation[] annotations,
                        MediaType mediaType) {

        // Deprecated by JAX-RS 2.0 and ignored by Jersey runtime.
        return -1;
    }

    @Override
    public void writeTo(List<? extends Mappable> t,
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

    private void writeYamlTo(List<? extends Mappable> t, OutputStream entityStream) throws IOException {
        try (YamlWriter writer = new YamlWriter(entityStream)) {
            writer.writeAll(t);
        }
    }

    private void writeJsonTo(List<? extends Mappable> t, OutputStream entityStream) {
        try (JsonWriter writer = writerFactory(uriInfo).createWriter(entityStream)) {
            writer.write(JsonWriting.writeAll(t));
        }
    }
}
