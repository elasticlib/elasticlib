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
package org.elasticlib.node.components;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.elasticlib.common.hash.Guid;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import org.elasticlib.node.dao.AttributesDao;
import org.elasticlib.node.manager.ManagerModule;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Local node GUID provider integration tests.
 */
@Test(singleThreaded = true)
public class NodeGuidProviderTest {

    private Path path;
    private ManagerModule managerModule;
    private NodeGuidProvider nodeGuidProvider;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        start();
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        stop();
        recursiveDelete(path);
    }

    private void start() {
        managerModule = new ManagerModule(path.resolve("home"), config());
        nodeGuidProvider = new NodeGuidProvider(new AttributesDao(managerModule.getStorageManager()));

        managerModule.start();
        managerModule.getStorageManager().inTransaction(nodeGuidProvider::start);
    }

    private void stop() {
        nodeGuidProvider.stop();
        managerModule.stop();
    }

    /**
     * Test.
     */
    @Test
    public void guidTest() {
        assertThat(nodeGuidProvider.guid()).isNotNull();
    }

    /**
     * Test.
     */
    @Test
    public void guidAfterRestartTest() {
        Guid expected = nodeGuidProvider.guid();
        stop();
        start();

        assertThat(nodeGuidProvider.guid()).isEqualTo(expected);
    }
}
