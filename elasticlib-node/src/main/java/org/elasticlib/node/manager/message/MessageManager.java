/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.manager.message;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static java.util.Collections.emptySet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.elasticlib.node.manager.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Class<?>, Set<Action<?>>> actions = new HashMap<>();

    /**
     * Constructor.
     *
     * @param taskManager Asynchronous tasks manager.
     */
    public MessageManager(TaskManager taskManager) {
        this.taskManager = taskManager;
    }

    /**
     * Registers an action to apply each time a message of the supplied type is posted. Different actions may be
     * registered for a given message type. However, if the an action is registered twice, the second registration will
     * be ignored.
     *
     * @param <T> Class of the message to listen to.
     * @param messageType Message type to listen to.
     * @param action Associated action to register.
     */
    public <T> void register(Class<T> messageType, Action<T> action) {
        LOG.info("Registering [{}] => {}", format(messageType), action.description());
        lock.writeLock().lock();
        try {
            if (!actions.containsKey(messageType)) {
                actions.put(messageType, new CopyOnWriteArraySet<>());
            }
            actions.get(messageType).add(action);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Posts a message, applying all previously registered actions to its type. Each action is asynchronously applied in
     * a separate task.
     *
     * @param message The message to post.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void post(Object message) {
        LOG.info("Receiving [{}]", format(message.getClass()));
        actions(message.getClass()).forEach((Action action) -> {
            taskManager.execute(action.description(), () -> action.apply(message));
        });
    }

    private <T> Set<Action<?>> actions(Class<T> messageType) {
        lock.readLock().lock();
        try {
            if (!actions.containsKey(messageType)) {
                return emptySet();
            }
            return actions.get(messageType);

        } finally {
            lock.readLock().unlock();
        }
    }

    private static String format(Class<?> messageType) {
        return UPPER_CAMEL.to(LOWER_HYPHEN, messageType.getSimpleName());
    }
}
