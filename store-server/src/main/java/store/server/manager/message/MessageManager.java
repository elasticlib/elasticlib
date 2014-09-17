package store.server.manager.message;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.server.manager.task.TaskManager;

/**
 * Provides infrastructure for a message-driven architecture.
 * <p>
 * On the one hand, sender components may post messages to indicate a state change. On the other hand, receiver
 * components may register actions to be applied when a message is posted. Dispatching of the messages is handled by
 * this manager.
 * <p>
 * This allows unrelated components to communicate in a loosely-coupled fashion : senders and receivers do not have to
 * know each other.
 */
public class MessageManager {

    private static final Logger LOG = LoggerFactory.getLogger(MessageManager.class);
    private final TaskManager taskManager;
    private final Map<MessageType, Set<Action>> receivers;

    /**
     * Constructor.
     *
     * @param taskManager Asynchronous tasks manager.
     */
    public MessageManager(TaskManager taskManager) {
        this.taskManager = taskManager;
        receivers = new EnumMap<>(MessageType.class);
        for (MessageType type : MessageType.values()) {
            receivers.put(type, new CopyOnWriteArraySet<Action>());
        }
    }

    /**
     * Registers an action to apply each time a message of the supplied type is posted. Different actions may be
     * registered for a given message type. However, if the same action is registered twice, the second registration
     * will be ignored.
     *
     * @param type Type to listen to.
     * @param action Action to apply.
     */
    public void register(MessageType type, Action action) {
        LOG.info("Registering [{}] => {}", type, action.description());
        receivers.get(type).add(action);
    }

    /**
     * Posts a message, applying all previously registered actions for its associated message type. Each action is
     * asynchronously applied in a separate task.
     *
     * @param message The message to post.
     */
    public void post(final Message message) {
        LOG.info("Receiving [{}]", message.getType());
        for (final Action action : receivers.get(message.getType())) {
            taskManager.execute(action.description(), new Runnable() {
                @Override
                public void run() {
                    action.apply(message);
                }
            });
        }
    }
}
