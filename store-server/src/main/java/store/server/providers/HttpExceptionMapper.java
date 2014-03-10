package store.server.providers;

import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import static javax.ws.rs.core.Response.Status.*;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import store.server.exception.BadRequestException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.RevSpecCheckingFailedException;
import store.server.exception.SelfReplicationException;
import store.server.exception.ServerException;
import store.server.exception.UnknownContentException;
import store.server.exception.UnknownRepositoryException;
import store.server.exception.UnknownRevisionException;
import store.server.exception.WriteException;

/**
 * Map exceptions to HTTP responses.
 */
@Provider
public class HttpExceptionMapper implements ExceptionMapper<ServerException> {

    private static final Map<Class<?>, Status> MAPPING = new HashMap<>();

    static {
        MAPPING.put(BadRequestException.class, BAD_REQUEST);

        MAPPING.put(RepositoryAlreadyExistsException.class, PRECONDITION_FAILED);
        MAPPING.put(IntegrityCheckingFailedException.class, PRECONDITION_FAILED);
        MAPPING.put(InvalidRepositoryPathException.class, PRECONDITION_FAILED);
        MAPPING.put(SelfReplicationException.class, PRECONDITION_FAILED);
        MAPPING.put(UnknownRevisionException.class, PRECONDITION_FAILED);

        MAPPING.put(RevSpecCheckingFailedException.class, CONFLICT);

        MAPPING.put(UnknownRepositoryException.class, NOT_FOUND);
        MAPPING.put(UnknownContentException.class, NOT_FOUND);

        MAPPING.put(RepositoryNotStartedException.class, SERVICE_UNAVAILABLE);

        MAPPING.put(WriteException.class, INTERNAL_SERVER_ERROR);
    }

    private static Status of(ServerException exception) {
        Status status = MAPPING.get(exception.getClass());
        return status != null ? status : INTERNAL_SERVER_ERROR;
    }

    @Override
    public Response toResponse(ServerException exception) {
        return Response
                .status(of(exception))
                .entity(Json.createObjectBuilder().add("error", exception.getMessage()).build())
                .build();
    }
}
