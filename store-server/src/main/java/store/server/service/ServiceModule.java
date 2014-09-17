package store.server.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.config.Config;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.exception.WriteException;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.TaskManager;

/**
 * Manages services life-cycle.
 */
public class ServiceModule {

    private static final String STORAGE = "storage";
    private static final String SERVICES = "services";
    private final TaskManager taskManager;
    private final StorageManager storageManager;
    private final RepositoriesService repositoriesService;
    private final NodesService nodesService;

    /**
     * Constructor. Build and starts all services.
     *
     * @param home Server home directory path.
     * @param config Server config.
     */
    public ServiceModule(Path home, Config config) {
        taskManager = new TaskManager(config);
        storageManager = newStorageManager(home.resolve(STORAGE), config, taskManager);

        AttributesDao attributesDao = new AttributesDao(storageManager);
        NodesDao nodesDao = new NodesDao(storageManager);
        RepositoriesDao repositoriesDao = new RepositoriesDao(storageManager);
        ReplicationsDao replicationsDao = new ReplicationsDao(storageManager);

        NodeNameProvider nodeNameProvider = new NodeNameProvider(config);
        PublishUrisProvider publishUrisProvider = new PublishUrisProvider(config);
        NodePingHandler nodePingHandler = new NodePingHandler();

        repositoriesService = new RepositoriesService(config,
                                                      taskManager,
                                                      storageManager,
                                                      repositoriesDao,
                                                      replicationsDao);

        nodesService = new NodesService(config,
                                        taskManager,
                                        storageManager,
                                        attributesDao,
                                        nodesDao,
                                        nodeNameProvider,
                                        publishUrisProvider,
                                        nodePingHandler);
    }

    private static StorageManager newStorageManager(Path path, Config config, TaskManager asyncManager) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        return new StorageManager(SERVICES, path, config, asyncManager);
    }

    /**
     * Starts the module.
     */
    public void start() {
        repositoriesService.start();
        nodesService.start();
    }

    /**
     * Properly stops all services.
     */
    public void stop() {
        nodesService.stop();
        repositoriesService.stop();
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
     * @return The repositories service.
     */
    public RepositoriesService getRepositoriesService() {
        return repositoriesService;
    }

    /**
     * @return The nodes service.
     */
    public NodesService getNodesService() {
        return nodesService;
    }
}
