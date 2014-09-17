package store.server.manager.message;

/**
 * Represents a message, encapsulating information about a state change.
 */
public interface Message {

    /**
     * @return The type of the message.
     */
    MessageType getType();
}
