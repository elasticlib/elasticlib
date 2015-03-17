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
package org.elasticlib.node.service;

import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.node.manager.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides info about the local node.
 */
public class NodeService {

    private static final Logger LOG = LoggerFactory.getLogger(NodeService.class);

    private final StorageManager storageManager;
    private final LocalRepositoriesPool localRepositoriesPool;
    private final NodeNameProvider nodeNameProvider;
    private final NodeGuidProvider nodeGuidProvider;
    private final PublishUrisProvider publishUrisProvider;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param localRepositoriesPool Local repositories pool.
     * @param nodeNameProvider Local node name provider.
     * @param nodeGuidProvider Local node GUID provider.
     * @param publishUrisProvider Local node publish URI(s) provider.
     */
    public NodeService(StorageManager storageManager,
                       LocalRepositoriesPool localRepositoriesPool,
                       NodeNameProvider nodeNameProvider,
                       NodeGuidProvider nodeGuidProvider,
                       PublishUrisProvider publishUrisProvider) {

        this.storageManager = storageManager;
        this.localRepositoriesPool = localRepositoriesPool;
        this.nodeNameProvider = nodeNameProvider;
        this.nodeGuidProvider = nodeGuidProvider;
        this.publishUrisProvider = publishUrisProvider;
    }

    /**
     * Starts this service.
     */
    public void start() {
        storageManager.inTransaction(nodeGuidProvider::start);
    }

    /**
     * Properly stops this service.
     */
    public void stop() {
        nodeGuidProvider.stop();
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
        return new NodeDef(nodeNameProvider.name(), nodeGuidProvider.guid(), publishUrisProvider.uris());
    }
}
