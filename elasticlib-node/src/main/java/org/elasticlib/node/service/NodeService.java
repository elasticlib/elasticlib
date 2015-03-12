package org.elasticlib.node.service;

import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.node.dao.AttributesDao;
import org.elasticlib.node.manager.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides info about the local node.
 */
public class NodeService {

    private static final Logger LOG = LoggerFactory.getLogger(NodeService.class);

    private final StorageManager storageManager;
    private final AttributesDao attributesDao;
    private final LocalRepositoriesPool localRepositoriesPool;
    private final NodeNameProvider nodeNameProvider;
    private final PublishUrisProvider publishUrisProvider;
    private Guid guid;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param attributesDao Attributes DAO.
     * @param localRepositoriesPool Local repositories pool.
     * @param nodeNameProvider Node name provider.
     * @param publishUrisProvider Publish URI(s) provider.
     */
    public NodeService(StorageManager storageManager,
                       AttributesDao attributesDao,
                       LocalRepositoriesPool localRepositoriesPool,
                       NodeNameProvider nodeNameProvider,
                       PublishUrisProvider publishUrisProvider) {

        this.storageManager = storageManager;
        this.attributesDao = attributesDao;
        this.localRepositoriesPool = localRepositoriesPool;
        this.nodeNameProvider = nodeNameProvider;
        this.publishUrisProvider = publishUrisProvider;
    }

    /**
     * Starts this service.
     */
    public void start() {
        guid = storageManager.inTransaction(attributesDao::guid);
    }

    /**
     * Properly stops this service.
     */
    public void stop() {
        // Nothing to do.
    }

    /**
     * @return The GUID of the local node.
     */
    public Guid getGuid() {
        LOG.info("Returning local node GUID");
        return guid;
    }

    /**
     * @return The definition of the local node.
     */
    public NodeDef getNodeDef() {
        LOG.info("Returning local node definition");
        return nodeDef();
    }

    /**
     * @return Info about the local node.
     */
    public NodeInfo getNodeInfo() {
        LOG.info("Returning local node info");
        return new NodeInfo(nodeDef(),
                            storageManager.inTransaction(localRepositoriesPool::listRepositoryInfos));
    }

    private NodeDef nodeDef() {
        return new NodeDef(nodeNameProvider.name(), guid, publishUrisProvider.uris());
    }
}
