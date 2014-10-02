package store.server.manager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.config.Config;
import store.common.exception.IOFailureException;
import store.server.manager.message.MessageManager;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.TaskManager;

/**
 * Manages managers life-cycle.
 */
public class ManagerModule {

    private static final String STORAGE = "storage";
    private static final String SERVICES = "services";
    private final TaskManager taskManager;
    private final StorageManager storageManager;
    private final MessageManager messageManager;

    /**
     * Constructor. Build and starts all managers.
     *
     * @param home Home directory path.
     * @param config Configuration holder.
     */
    public ManagerModule(Path home, Config config) {
        taskManager = new TaskManager(config);
        storageManager = newStorageManager(home.resolve(STORAGE), config, taskManager);
        messageManager = new MessageManager(taskManager);
    }

    private static StorageManager newStorageManager(Path path, Config config, TaskManager asyncManager) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
        } catch (IOException e) {
            throw new IOFailureException(e);
        }
        return new StorageManager(SERVICES, path, config, asyncManager);
    }

    /**
     * Starts the module.
     */
    public void start() {
        // Actually does nothing. This method only exists for consistency with other modules.
    }

    /**
     * Properly stops all managers.
     */
    public void stop() {
        storageManager.stop();
        taskManager.stop();
    }

    /**
     * @return The asynchronous tasks manager.
     */
    public TaskManager getTaskManager() {
        return taskManager;
    }

    /**
     * @return The persistent storage provider.
     */
    public StorageManager getStorageManager() {
        return storageManager;
    }

    /**
     * @return The messaging infrastructure manager.
     */
    public MessageManager getMessageManager() {
        return messageManager;
    }
}
