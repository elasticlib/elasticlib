package store.server.service;

import store.common.config.Config;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.manager.ManagerModule;
import store.server.manager.message.MessageManager;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.TaskManager;

/**
 * Manages services life-cycle.
 */
public class ServiceModule {

    private final RepositoriesService repositoriesService;
    private final ReplicationsService replicationsService;
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
        MessageManager messageManager = managerModule.getMessageManager();

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
                                                      messageManager,
                                                      repositoriesDao);

        replicationsService = new ReplicationsService(storageManager,
                                                      messageManager,
                                                      repositoriesDao,
                                                      replicationsDao,
                                                      repositoriesService);

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
        replicationsService.start();
        nodesService.start();
    }

    /**
     * Properly stops all services.
     */
    public void stop() {
        nodesService.stop();
        replicationsService.stop();
        repositoriesService.stop();
    }

    /**
     * @return The repositories service.
     */
    public RepositoriesService getRepositoriesService() {
        return repositoriesService;
    }

    /**
     * @return The replications service.
     */
    public ReplicationsService getReplicationsService() {
        return replicationsService;
    }

    /**
     * @return The nodes service.
     */
    public NodesService getNodesService() {
        return nodesService;
    }
}