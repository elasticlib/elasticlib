package store.server.providers;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import org.jvnet.mimepull.MIMEConfig;
import org.jvnet.mimepull.MIMEMessage;
import store.server.multipart.Multipart;

/**
 * Multipart reader.
 */
@Consumes("multipart/*")
public class MultipartReader implements MessageBodyReader<Multipart> {

    private final MIMEConfig mimeConfig = new MIMEConfig();

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Multipart.class.isAssignableFrom(type);
    }

    @Override
    public Multipart readFrom(Class<Multipart> type,
                              Type genericType,
                              Annotation[] annotations,
                              MediaType mediaType,
                              MultivaluedMap<String, String> httpHeaders,
                              InputStream entityStream) throws IOException, WebApplicationException {

        return new Multipart(new MIMEMessage(entityStream,
                                             mediaType.getParameters().get("boundary"),
                                             mimeConfig));
    }
}
