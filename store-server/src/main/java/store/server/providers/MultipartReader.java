package store.server.providers;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import static org.glassfish.jersey.message.internal.MediaTypes.typeEqual;
import org.jvnet.mimepull.MIMEConfig;
import org.jvnet.mimepull.MIMEMessage;
import store.server.multipart.FormDataMultipart;
import store.server.multipart.Multipart;

/**
 * Multipart reader.
 */
@Provider
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

        MIMEMessage mimeMessage = new MIMEMessage(entityStream,
                                                  mediaType.getParameters().get("boundary"),
                                                  mimeConfig);

        if (typeEqual(mediaType, MediaType.MULTIPART_FORM_DATA_TYPE)) {
            // see if the User-Agent header corresponds to some version of MS Internet Explorer
            // if so, need to set fileNameFix to true to handle issue http://java.net/jira/browse/JERSEY-759
            String userAgent = httpHeaders.getFirst(HttpHeaders.USER_AGENT);
            boolean fileNameFix = userAgent != null && userAgent.contains(" MSIE ");
            return new FormDataMultipart(mimeMessage, fileNameFix);
        }
        return new Multipart(mimeMessage);
    }
}
