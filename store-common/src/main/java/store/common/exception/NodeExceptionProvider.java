package store.common.exception;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Node exceptions instances provider.
 */
class NodeExceptionProvider {

    private static final Map<Integer, Class<? extends NodeException>> CLASSES = new HashMap<>();

    static {
        register(BadRequestException.class,
                 RepositoryAlreadyExistsException.class,
                 IntegrityCheckingFailedException.class,
                 InvalidRepositoryPathException.class,
                 SelfReplicationException.class,
                 UnknownRevisionException.class,
                 NodeAlreadyTrackedException.class,
                 SelfTrackingException.class,
                 UnknownRepositoryException.class,
                 UnknownReplicationException.class,
                 UnknownContentException.class,
                 UnknownNodeException.class,
                 ConflictException.class,
                 RepositoryClosedException.class,
                 TransactionNotFoundException.class,
                 UnreachableNodeException.class,
                 NodeFailureException.class,
                 IOFailedException.class);
    }

    private NodeExceptionProvider() {
    }

    @SafeVarargs
    private static void register(Class<? extends NodeException>... classes) {
        try {
            for (Class<? extends NodeException> clazz : classes) {
                int code = clazz.getConstructor().newInstance().getCode();
                CLASSES.put(code, clazz);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Create a new instance of the actual class of the exception associated with supplied code. If the class of the
     * returned exception supports message injection in constructor, supplied message is set on returned instance.
     *
     * @param code A NodeException code.
     * @param message Exception message.
     * @return A new instance of the corresponding exception class.
     */
    public static NodeException newInstance(int code, String message) {
        try {
            Class<? extends NodeException> clazz = CLASSES.get(code);
            Constructor<? extends NodeException> constructorWithMessage = constructorWithMessage(clazz);
            if (constructorWithMessage != null) {
                return constructorWithMessage.newInstance(message);
            }
            return clazz.getConstructor().newInstance();

        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Constructor<? extends NodeException> constructorWithMessage(Class<? extends NodeException> clazz)
            throws NoSuchMethodException {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            if (parameterTypes.length == 1 && parameterTypes[0] == String.class) {
                return clazz.getConstructor(String.class);
            }
        }
        return null;
    }
}
