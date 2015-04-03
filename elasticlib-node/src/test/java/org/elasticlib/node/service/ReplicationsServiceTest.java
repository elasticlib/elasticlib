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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.ReplicationAlreadyExistsException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.SelfReplicationException;
import org.elasticlib.common.exception.UnknownReplicationException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.value.Value;
import org.elasticlib.node.TestContent;
import static org.elasticlib.node.TestUtil.LOREM_IPSUM;
import static org.elasticlib.node.TestUtil.async;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import org.elasticlib.node.components.LocalRepositoriesFactory;
import org.elasticlib.node.components.LocalRepositoriesPool;
import org.elasticlib.node.components.RemoteRepositoriesPool;
import org.elasticlib.node.components.ReplicationAgentsPool;
import org.elasticlib.node.components.RepositoriesProvider;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.dao.ReplicationsDao;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.repository.Repository;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Replications service integration tests.
 */
public class ReplicationsServiceTest {

    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";

    private Path path;
    private ManagerModule managerModule;
    private LocalRepositoriesPool localRepositoriesPool;
    private RemoteRepositoriesPool remoteRepositoriesPool;
    private ReplicationAgentsPool replicationAgentsPool;
    private RepositoriesService repositoriesService;
    private ReplicationsService replicationsService;
    private volatile TestContent content;
    private volatile Guid guid;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        Config config = config();

        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        managerModule = new ManagerModule(path.resolve("home"), config);

        RepositoriesDao repositoriesDao = new RepositoriesDao(managerModule.getStorageManager());
        ReplicationsDao replicationsDao = new ReplicationsDao(managerModule.getStorageManager());
        RemotesDao remotesDao = new RemotesDao(managerModule.getStorageManager());

        LocalRepositoriesFactory factory = new LocalRepositoriesFactory(config,
                                                                        managerModule.getTaskManager(),
                                                                        managerModule.getMessageManager());

        localRepositoriesPool = new LocalRepositoriesPool(repositoriesDao, factory);
        remoteRepositoriesPool = new RemoteRepositoriesPool(managerModule.getClientsManager(),
                                                            managerModule.getMessageManager(),
                                                            remotesDao);

        RepositoriesProvider repositoriesProvider = new RepositoriesProvider(localRepositoriesPool,
                                                                             remoteRepositoriesPool);

        replicationAgentsPool = new ReplicationAgentsPool(managerModule.getStorageManager(),
                                                          repositoriesProvider);

        repositoriesService = new RepositoriesService(managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      localRepositoriesPool);

        replicationsService = new ReplicationsService(managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      replicationsDao,
                                                      repositoriesProvider,
                                                      replicationAgentsPool);
        managerModule.start();
        managerModule.getStorageManager().inTransaction(localRepositoriesPool::start);
        remoteRepositoriesPool.start();
        replicationAgentsPool.start();
        replicationsService.start();

        repositoriesService.createRepository(path.resolve(SOURCE));
        repositoriesService.createRepository(path.resolve(DESTINATION));

        content = LOREM_IPSUM;
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        replicationsService.stop();
        replicationAgentsPool.stop();
        remoteRepositoriesPool.stop();
        localRepositoriesPool.stop();
        managerModule.stop();
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test
    public void createReplicationTest() {
        addSourceContent();
        replicationsService.createReplication(SOURCE, DESTINATION);

        guid = replicationsService.listReplicationInfos().get(0).getGuid();

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = SelfReplicationException.class)
    public void createReplicationSelfTest() {
        replicationsService.createReplication(SOURCE, SOURCE);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownRepositoryException.class)
    public void createReplicationUnknownRepositoryTest() {
        replicationsService.createReplication(SOURCE, "unknown");
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "createReplicationTest", expectedExceptions = ReplicationAlreadyExistsException.class)
    public void createReplicationAlreadyExistingTest() {
        replicationsService.createReplication(SOURCE, DESTINATION);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "createReplicationTest")
    public void newRepositoryEventTest() {
        updateSourceContent("newRepositoryEventTest");
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "newRepositoryEventTest")
    public void stopReplicationTest() {
        replicationsService.stopReplication(guid);
        TestContent previousContent = content;
        updateSourceContent("stopReplicationTest");

        assertReplicationStopped();
        assertDestinationHas(previousContent);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "stopReplicationTest")
    public void startReplicationTest() {
        replicationsService.startReplication(guid);

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "stopReplicationTest", expectedExceptions = UnknownReplicationException.class)
    public void startReplicationUnknownTest() {
        replicationsService.startReplication(unknown());
    }

    private Guid unknown() {
        Guid unknown = Guid.random();
        while (unknown.equals(guid)) {
            unknown = Guid.random();
        }
        return unknown;
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "startReplicationTest")
    public void closeRepositoryTest() {
        repositoriesService.closeRepository(DESTINATION);
        async(this::assertReplicationStopped);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "closeRepositoryTest", expectedExceptions = RepositoryClosedException.class)
    public void startReplicationWithClosedRepositoryTest() {
        replicationsService.startReplication(guid);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "startReplicationWithClosedRepositoryTest")
    public void openRepositoryTest() {
        repositoriesService.openRepository(DESTINATION);
        updateSourceContent("openRepositoryTest");

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "openRepositoryTest")
    public void removeSourceRepositoryTest() {
        removeRepositoryTest(SOURCE);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "removeSourceRepositoryTest")
    public void removeDestinationRepositoryTest() {
        removeRepositoryTest(DESTINATION);
    }

    private void removeRepositoryTest(String repositoryName) {
        repositoriesService.removeRepository(repositoryName);
        async(() -> {
            assertThat(replicationsService.listReplicationInfos()).isEmpty();
        });

        // Restore previous state.
        repositoriesService.addRepository(path.resolve(repositoryName));
        replicationsService.createReplication(SOURCE, DESTINATION);
        guid = replicationsService.listReplicationInfos().get(0).getGuid();

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "removeDestinationRepositoryTest")
    public void deleteReplicationTest() {
        replicationsService.deleteReplication(guid);
        assertThat(replicationsService.listReplicationInfos()).isEmpty();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "deleteReplicationTest", expectedExceptions = UnknownReplicationException.class)
    public void deleteReplicationUnknownTest() {
        replicationsService.deleteReplication(guid);
    }

    private ReplicationInfo replicationInfo() {
        List<ReplicationInfo> infos = replicationsService.listReplicationInfos();
        assertThat(infos).hasSize(1);

        return infos.get(0);
    }

    private void addSourceContent() {
        Repository repository = repositoriesService.getRepository(SOURCE);

        try (InputStream inputStream = content.getInputStream()) {
            StagingInfo stagingInfo = repository.stageContent(content.getHash());
            repository.writeContent(content.getHash(), stagingInfo.getSessionId(), inputStream, 0);

        } catch (IOException e) {
            throw new AssertionError(e);
        }
        CommandResult result = repository.addRevision(content.getRevision());
        assertThat(result.getOperation()).isEqualTo(Operation.CREATE);
    }

    private void updateSourceContent(String newValue) {
        content = content.add("test", Value.of(newValue));
        repositoriesService.getRepository(SOURCE).addRevision(content.getRevision());
    }

    private void assertReplicationStarted() {
        assertReplicationStarted(true);
    }

    private void assertReplicationStopped() {
        assertReplicationStarted(false);
    }

    private void assertReplicationStarted(boolean expected) {
        ReplicationInfo info = replicationInfo();

        assertThat(info.isStarted()).isEqualTo(expected);
        assertThat(info.getSourceDef().get().getName()).isEqualTo(SOURCE);
        assertThat(info.getDestinationdef().get().getName()).isEqualTo(DESTINATION);
    }

    private void assertDestinationUpToDate() {
        async(() -> {
            AgentInfo info = replicationInfo().getAgentInfo();
            assertThat(info.getState()).isEqualTo(AgentState.WAITING);
            assertThat(info.getCurSeq()).isEqualTo(info.getMaxSeq());

            assertDestinationHas(content);
        });
    }

    private void assertDestinationHas(TestContent content) {
        RevisionTree actual = repositoriesService.getRepository(DESTINATION).getTree(content.getHash());
        assertThat(actual).isEqualTo(content.getTree());
    }
}
