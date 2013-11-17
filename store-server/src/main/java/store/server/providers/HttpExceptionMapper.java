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
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.IndexAlreadyExistsException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreException;
import store.server.exception.UnknownHashException;
import store.server.exception.UnknownIndexException;
import store.server.exception.UnknownVolumeException;
import store.server.exception.VolumeNotStartedException;
import store.server.exception.WriteException;

/**
 * Map exceptions to HTTP responses.
 */
@Provider
public class HttpExceptionMapper implements ExceptionMapper<StoreException> {

    private static final Map<Class<?>, Status> MAPPING = new HashMap<>();

    static {
        MAPPING.put(BadRequestException.class, BAD_REQUEST);

        MAPPING.put(ContentAlreadyStoredException.class, PRECONDITION_FAILED);
        MAPPING.put(IndexAlreadyExistsException.class, PRECONDITION_FAILED);
        MAPPING.put(IntegrityCheckingFailedException.class, PRECONDITION_FAILED);
        MAPPING.put(InvalidStorePathException.class, PRECONDITION_FAILED);

        MAPPING.put(UnknownHashException.class, NOT_FOUND);
        MAPPING.put(UnknownIndexException.class, NOT_FOUND);
        MAPPING.put(UnknownVolumeException.class, NOT_FOUND);

        MAPPING.put(VolumeNotStartedException.class, SERVICE_UNAVAILABLE);

        MAPPING.put(WriteException.class, INTERNAL_SERVER_ERROR);
    }

    private static Status of(StoreException exception) {
        Status status = MAPPING.get(exception.getClass());
        return status != null ? status : INTERNAL_SERVER_ERROR;
    }

    @Override
    public Response toResponse(StoreException exception) {
        return Response
                .status(of(exception))
                .entity(Json.createObjectBuilder().add("error", exception.getMessage()).build())
                .build();
    }
}
