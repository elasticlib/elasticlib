package store.common.exception;

import static java.lang.Integer.parseInt;
import java.util.Map;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.value.Value;

/**
 * Base class for all exceptions thrown by a node.
 */
public abstract class NodeException extends RuntimeException implements Mappable {

    private static final String STATUS = "status";
    private static final String CODE = "code";
    private static final String MESSAGE = "message";
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public NodeException() {
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public NodeException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public NodeException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     * @param cause Cause exception.
     */
    public NodeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Provides a unique code identifying this exception type, The 3 first digits of this code matches the HTTP response
     * status associated with this exception.
     *
     * @return This exception code.
     */
    public abstract int getCode();

    /**
     * Helper method to build a code.
     *
     * @param status Code associated HTTP response status.
     * @param suffix Unique suffix among all exceptions with supplied status.
     * @return Corresponding node exception code.
     */
    protected final int code(StatusType status, int suffix) {
        return parseInt(Integer.toString(status.getStatusCode()) + suffix);
    }

    /**
     * Provides the HTTP response status associated with this exception.
     *
     * @return A HTTP response status.
     */
    public final StatusType getStatus() {
        int statusCode = parseInt(Integer.toString(getCode()).substring(0, 3));
        return Status.fromStatusCode(statusCode);
    }

    @Override
    public final Map<String, Value> toMap() {
        return new MapBuilder()
                .put(STATUS, getStatus().getReasonPhrase())
                .put(CODE, getCode())
                .put(MESSAGE, getMessage())
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static NodeException fromMap(Map<String, Value> map) {
        int code = (int) map.get(CODE).asLong();
        String message = map.get(MESSAGE).asString();
        return NodeExceptionProvider.newInstance(code, message);
    }
}
