package org.elasticlib.node.service;

import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.node.dao.AttributesDao;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides info about the local node.
 */
public class NodeService {

    private final StorageManager storageManager;
    private final AttributesDao attributesDao;
    private final NodeNameProvider nodeNameProvider;
    private final PublishUrisProvider publishUrisProvider;
    private Guid guid;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param attributesDao Attributes DAO.
     * @param nodeNameProvider Node name provider.
     * @param publishUrisProvider Publish URI(s) provider.
     */
    public NodeService(StorageManager storageManager,
                       AttributesDao attributesDao,
                       NodeNameProvider nodeNameProvider,
                       PublishUrisProvider publishUrisProvider) {

        this.storageManager = storageManager;
        this.attributesDao = attributesDao;
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
        return guid;
    }

    /**
     * @return The definition of the local node.
     */
    public NodeDef getNodeDef() {
        return new NodeDef(nodeNameProvider.name(), guid, publishUrisProvider.uris());
    }
}
