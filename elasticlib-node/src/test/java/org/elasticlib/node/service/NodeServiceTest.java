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

import static com.sleepycat.je.EnvironmentConfig.LOG_MEM_ONLY;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import static org.elasticlib.node.config.NodeConfig.NODE_NAME;
import static org.elasticlib.node.config.NodeConfig.NODE_URIS;
import org.elasticlib.node.dao.AttributesDao;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.manager.ManagerModule;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Node service integration tests.
 */
@Test(singleThreaded = true)
public class NodeServiceTest {

    private static final String NAME = "test";
    private static final String LOCALHOST = "http://localhost:9400";

    private Path path;
    private ManagerModule managerModule;
    private LocalRepositoriesPool localRepositoriesPool;
    private NodeGuidProvider nodeGuidProvider;
    private NodeService nodeService;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        recursiveDelete(path);
    }

    /**
     * Creates and starts services.
     */
    @BeforeMethod
    public void startServices() {
        Config config = config()
                .set(NODE_NAME, NAME)
                .set(NODE_URIS, LOCALHOST)
                .set(LOG_MEM_ONLY, "true");

        managerModule = new ManagerModule(path.resolve("home"), config);

        localRepositoriesPool = new LocalRepositoriesPool(
                new RepositoriesDao(managerModule.getStorageManager()),
                new LocalRepositoriesFactory(config,
                                             managerModule.getTaskManager(),
                                             managerModule.getMessageManager()));

        nodeGuidProvider = new NodeGuidProvider(new AttributesDao(managerModule.getStorageManager()));

        nodeService = new NodeService(managerModule.getStorageManager(),
                                      localRepositoriesPool,
                                      new NodeNameProvider(config),
                                      nodeGuidProvider,
                                      new PublishUrisProvider(config));

        managerModule.start();
        managerModule.getStorageManager().inTransaction(localRepositoriesPool::start);
        nodeService.start();
    }

    /**
     * Stops services.
     */
    @AfterMethod
    public void stopServices() {
        nodeService.stop();
        localRepositoriesPool.stop();
        managerModule.stop();
    }

    /**
     * Test.
     */
    @Test
    public void getNodeDefTest() {
        assertThat(nodeService.getNodeDef()).isEqualTo(expectedDef());
    }

    /**
     * Test.
     */
    @Test
    public void getNodeInfoTest() {
        assertThat(nodeService.getNodeInfo()).isEqualTo(expectedInfo());
    }

    private NodeDef expectedDef() {
        return new NodeDef(NAME, nodeGuidProvider.guid(), singletonList(URI.create(LOCALHOST)));
    }

    private NodeInfo expectedInfo() {
        return new NodeInfo(expectedDef(), emptyList());
    }
}
