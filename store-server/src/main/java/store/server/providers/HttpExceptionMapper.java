package store.server.providers;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.exception.NodeException;
import store.common.exception.UnexpectedFailureException;

/**
 * Handles exceptions that happen while servicing HTTP requests.
 * <p>
 * Builds an adequate response to return to the client. Additionally, if the exception is not an expected one, logs its
 * stacktrace for debugging purposes.
 */
@Provider
public class HttpExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpExceptionMapper.class);

    @Override
    public Response toResponse(Throwable throwable) {
        NodeException exception = asNodeException(throwable);
        return Response
                .status(exception.getStatus())
                .entity(exception)
                .build();
    }

    private static NodeException asNodeException(Throwable throwable) {
        if (throwable instanceof NodeException) {
            return (NodeException) throwable;
        }
        LOG.error("An unexpected error happened", throwable);
        return new UnexpectedFailureException(throwable);
    }
}
