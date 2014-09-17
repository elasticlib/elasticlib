package store.server.message;

/**
 * Represents an action to apply when a message is posted.
 */
public interface Action {

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
    void apply(Message message);
}
