package store.server.service;

import store.common.config.Config;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.manager.ManagerModule;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.TaskManager;

/**
 * Manages services life-cycle.
 */
public class ServiceModule {

    private final RepositoriesService repositoriesService;
    private final NodesService nodesService;

    /**
     * Constructor. Build and starts all services.
     *
     * @param config Configuration holder.
     * @param managerModule Manager module.
     */
    public ServiceModule(Config config, ManagerModule managerModule) {
        TaskManager taskManager = managerModule.getTaskManager();
        StorageManager storageManager = managerModule.getStorageManager();

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
