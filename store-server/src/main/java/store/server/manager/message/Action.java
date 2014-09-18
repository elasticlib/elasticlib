package store.server.manager.message;

/**
 * Represents an action to apply when a message is posted.
 *
 * @param <T> Type of the message this action is associated to.
 */
public interface Action<T> {

    /**
     * Provides a short description of this action, intended for logging purposes.
     *
     * @return A description of this action.
     */
    String description();

    /**
     * Apply this action.
     *
     * @param message The message sent.
     */
    void apply(T message);
}
