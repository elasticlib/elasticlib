package store.server.providers;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.server.exception.BadRequestException;
import store.server.exception.ConflictException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.NodeAlreadyTrackedException;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.RepositoryClosedException;
import store.server.exception.SelfReplicationException;
import store.server.exception.SelfTrackingException;
import store.server.exception.ServerException;
import store.server.exception.TransactionNotFoundException;
import store.server.exception.UnknownContentException;
import store.server.exception.UnknownNodeException;
import store.server.exception.UnknownReplicationException;
import store.server.exception.UnknownRepositoryException;
import store.server.exception.UnknownRevisionException;
import store.server.exception.UnreachableNodeException;
import store.server.exception.WriteException;

/**
 * Handles exceptions that happen while servicing HTTP requests.
 * <p>
 * Builds an adequate response to return to the client. Additionally, if the exception is not an expected one, logs its
 * stacktrace for debugging purposes.
 */
@Provider
public class HttpExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpExceptionMapper.class);
    private static final Map<Class<?>, Status> MAPPING = new HashMap<>();

    static {
        MAPPING.put(BadRequestException.class, BAD_REQUEST);

        MAPPING.put(RepositoryAlreadyExistsException.class, PRECONDITION_FAILED);
        MAPPING.put(IntegrityCheckingFailedException.class, PRECONDITION_FAILED);
        MAPPING.put(InvalidRepositoryPathException.class, PRECONDITION_FAILED);
        MAPPING.put(SelfReplicationException.class, PRECONDITION_FAILED);
        MAPPING.put(UnknownRevisionException.class, PRECONDITION_FAILED);
        MAPPING.put(NodeAlreadyTrackedException.class, PRECONDITION_FAILED);
        MAPPING.put(SelfTrackingException.class, PRECONDITION_FAILED);

        MAPPING.put(ConflictException.class, CONFLICT);

        MAPPING.put(UnknownRepositoryException.class, NOT_FOUND);
        MAPPING.put(UnknownReplicationException.class, NOT_FOUND);
        MAPPING.put(UnknownContentException.class, NOT_FOUND);
        MAPPING.put(UnknownNodeException.class, NOT_FOUND);

        MAPPING.put(RepositoryClosedException.class, SERVICE_UNAVAILABLE);
        MAPPING.put(TransactionNotFoundException.class, SERVICE_UNAVAILABLE);
        MAPPING.put(UnreachableNodeException.class, SERVICE_UNAVAILABLE);

        MAPPING.put(WriteException.class, INTERNAL_SERVER_ERROR);
    }

    @Override
    public Response toResponse(Throwable exception) {
        if (!(exception instanceof ServerException)) {
            LOG.error("An unexpected error happened", exception);
        }
        return Response
                .status(of(exception))
                .entity(Json.createObjectBuilder().add("error", message(exception)).build())
                .build();
    }

    private static Status of(Throwable exception) {
        Status status = MAPPING.get(exception.getClass());
        return status != null ? status : INTERNAL_SERVER_ERROR;
    }

    private static String message(Throwable exception) {
        if (exception.getMessage() != null && !exception.getMessage().isEmpty()) {
            return exception.getMessage();
        }
        if (exception.getCause() != null) {
            return message(exception.getCause());
        }
        return toHumanCase(truncate(exception.getClass().getSimpleName(), "Exception"));
    }

    private static String truncate(String value, String suffix) {
        if (!value.endsWith(suffix)) {
            return value;
        }
        return value.substring(0, value.length() - suffix.length());
    }

    private static String toHumanCase(String className) {
        String lowerSpaced = UPPER_CAMEL.to(LOWER_HYPHEN, className).replace('-', ' ');
        return Character.toUpperCase(lowerSpaced.charAt(0)) + lowerSpaced.substring(1);
    }
}
