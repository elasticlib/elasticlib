package org.elasticlib.common.exception;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.common.base.Joiner;
import static com.google.common.base.Joiner.on;
import java.lang.reflect.Constructor;
import java.util.Map;
import javax.ws.rs.core.Response.StatusType;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.value.Value;

/**
 * Base class for all exceptions thrown by a node. These exceptions are mappable so they can be transmitted in HTTP
 * response body and restored on the client side.
 */
public abstract class NodeException extends RuntimeException implements Mappable {

    private static final String DOT = ".";
    private static final String DASH = " - ";
    private static final String STATUS = "status";
    private static final String ERROR = "error";
    private static final String MESSAGE = "message";
    private static final String EXCEPTION = "Exception";
    private static final long serialVersionUID = 1L;
    private static final String PACKAGE = NodeException.class.getPackage().getName();

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
     * Provides the HTTP response status associated with this exception.
     *
     * @return A HTTP response status.
     */
    public abstract StatusType getStatus();

    @Override
    public final Map<String, Value> toMap() {
        StatusType status = getStatus();
        return new MapBuilder()
                .put(STATUS, on(DASH).join(status.getStatusCode(), status.getReasonPhrase()))
                .put(ERROR, type(getClass()))
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
        return newInstance(map.get(ERROR).asString(),
                           map.get(MESSAGE).asString());
    }

    private static NodeException newInstance(String type, String message) {
        try {
            Class<?> clazz = loadClass(type);
            Constructor<?> withMsg = constructorWithMessage(clazz);
            if (withMsg != null) {
                return (NodeException) withMsg.newInstance(message);
            }
            return (NodeException) clazz.newInstance();

        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static String type(Class<?> clazz) {
        return truncate(clazz.getSimpleName(), EXCEPTION);
    }

    private static Class<?> loadClass(String type) throws ClassNotFoundException {
        return Class.forName(on("").join(PACKAGE, DOT, type, EXCEPTION));
    }

    private static Constructor<?> constructorWithMessage(Class<?> clazz) {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            if (parameterTypes.length == 1 && parameterTypes[0] == String.class) {
                return constructor;
            }
        }
        return null;
    }

    /**
     * Extracts message from supplied exception, containing its formatted name. Always returns a non empty string.
     *
     * @param exception An exception
     * @return a adequate message for this exception.
     */
    protected static String message(Throwable exception) {
        if (exception.getMessage() != null && !exception.getMessage().isEmpty()) {
            return Joiner.on(" - ").join(name(exception), exception.getMessage());
        }
        return name(exception);
    }

    private static String name(Throwable exception) {
        return toHumanCase(truncate(exception.getClass().getSimpleName(), EXCEPTION));
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
