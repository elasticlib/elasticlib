package store.common.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

/**
 * Ensure headers are not modified by any MessageBodyWriter.
 * <p>
 * Header modification is not supported by current transport connector (Apache HTTP). Therefore, without this
 * interceptor, warnings are raised because MultipartWriter attempts to set mime-version header.
 */
public class HeaderRestoringWriterInterceptor implements WriterInterceptor {

    /**
     * Constructor.
     */
    HeaderRestoringWriterInterceptor() {
        // This class is an implementation detail.
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException {
        MultivaluedMap<String, Object> before = context.getHeaders();
        context.proceed();
        MultivaluedMap<String, Object> after = context.getHeaders();
        after.clear();
        for (Map.Entry<String, List<Object>> entry : before.entrySet()) {
            after.addAll(entry.getKey(), entry.getValue());
        }
    }
}
