package store.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.config.Config;
import store.server.async.AsyncManager;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.exception.WriteException;
import store.server.service.NodesService;
import store.server.service.RepositoriesService;
import store.server.storage.StorageManager;

/**
 * Manages services life-cycle.
 */
public class ServicesContainer {

    private static final String STORAGE = "storage";
    private static final String SERVICES = "services";
    private final AsyncManager asyncManager;
    private final StorageManager storageManager;
    private final RepositoriesService repositoriesService;
    private final NodesService nodesService;

    /**
     * Constructor. Build and starts all services.
     *
     * @param home Server home directory path.
     * @param config Server config.
     */
    public ServicesContainer(Path home, Config config) {
        asyncManager = new AsyncManager(config);
        storageManager = newStorageManager(home.resolve(STORAGE), config, asyncManager);

        AttributesDao attributesDao = new AttributesDao(storageManager);
        NodesDao nodesDao = new NodesDao(storageManager);
        RepositoriesDao repositoriesDao = new RepositoriesDao(storageManager);
        ReplicationsDao replicationsDao = new ReplicationsDao(storageManager);

        repositoriesService = new RepositoriesService(config,
                                                      asyncManager,
                                                      storageManager,
                                                      repositoriesDao,
                                                      replicationsDao);

        nodesService = new NodesService(config, storageManager, nodesDao, attributesDao);
    }

    private static StorageManager newStorageManager(Path path, Config config, AsyncManager asyncManager) {
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
     * @return The asynchronous tasks manager.
     */
    public AsyncManager getAsyncManager() {
        return asyncManager;
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

    /**
     * Shutdown this container, properly closing all services.
     */
    public void shutdown() {
        asyncManager.close();
        repositoriesService.close();
        storageManager.close();
    }
}
