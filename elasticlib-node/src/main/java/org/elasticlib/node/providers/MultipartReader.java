package org.elasticlib.node.providers;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.elasticlib.node.multipart.FormDataMultipart;
import org.elasticlib.node.multipart.Multipart;
import static org.glassfish.jersey.message.internal.MediaTypes.typeEqual;
import org.glassfish.jersey.server.CloseableService;

/**
 * Multipart reader.
 */
@Provider
@Consumes("multipart/*")
public class MultipartReader implements MessageBodyReader<Multipart> {

    @Inject
    private javax.inject.Provider<CloseableService> closeableServiceProvider;

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
                              InputStream entityStream) {

        // Multipart implements Closeable and is responsible for closing entity inputStream.
        // This prevents Jersey runtime from closing entity inputStream directly when returning from this method,
        // which would break latter use of the returned multipart.
        Multipart multipart = newMultipart(mediaType, httpHeaders, entityStream);
        closeableServiceProvider.get().add(multipart);
        return multipart;
    }

    private Multipart newMultipart(MediaType mediaType,
                                   MultivaluedMap<String, String> httpHeaders,
                                   InputStream entityStream) {

        String boundary = mediaType.getParameters().get("boundary");
        if (typeEqual(mediaType, MediaType.MULTIPART_FORM_DATA_TYPE)) {
            // see if the User-Agent header corresponds to some version of MS Internet Explorer
            // if so, need to set fileNameFix to true to handle issue http://java.net/jira/browse/JERSEY-759
            String userAgent = httpHeaders.getFirst(HttpHeaders.USER_AGENT);
            boolean fileNameFix = userAgent != null && userAgent.contains(" MSIE ");
            return new FormDataMultipart(entityStream, boundary, fileNameFix);
        }
        return new Multipart(entityStream, boundary);
    }
}
