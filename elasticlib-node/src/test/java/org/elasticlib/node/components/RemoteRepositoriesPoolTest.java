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

import static com.sleepycat.je.EnvironmentConfig.LOG_MEM_ONLY;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.client.RepositoryTarget;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.exception.UnreachableNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.RepositoryStats;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.manager.client.ClientManager;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.repository.Repository;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Remote repositories pool unit tests.
 */
@Test(singleThreaded = true)
public class RemoteRepositoriesPoolTest {

    private static final String NODE_NAME = "remote";
    private static final Guid NODE_GUID = new Guid("88cd962fec779a3abafa95aad8ace74c");
    private static final URI NODE_URI = URI.create("http://localhost:9400");
    private static final String REPOSITORY_NAME = "repository";
    private static final Guid REPOSITORY_GUID = new Guid("eac7690f2ca05940e9239d5300037551");
    private static final String REPOSITORY_PATH = "/tmp/repository";

    private Path path;
    private StorageManager storageManager;
    private ClientManager clientManager;
    private RemotesDao remotesDao;
    private RemoteRepositoriesPool remoteRepositoriesPool;

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
     * Test set-up.
     */
    @BeforeMethod
    public void setUp() {
        storageManager = new StorageManager("storage", path, config().set(LOG_MEM_ONLY, "true"), null);
        remotesDao = new RemotesDao(storageManager);
        clientManager = mock(ClientManager.class);
        remoteRepositoriesPool = new RemoteRepositoriesPool(clientManager,
                                                            mock(MessageManager.class),
                                                            remotesDao);

        Client client = mock(Client.class, RETURNS_DEEP_STUBS);
        when(clientManager.getClient()).thenReturn(client);
        when(client.target(NODE_URI)
                .repositories()
                .get(REPOSITORY_GUID))
                .thenReturn(mock(RepositoryTarget.class));

        remoteRepositoriesPool.start();
    }

    /**
     * Test tear-down.
     */
    @AfterMethod
    public void tearDown() {
        remoteRepositoriesPool.stop();
        storageManager.stop();
    }

    /**
     * Data provider.
     *
     * @return getRepositoryGuid test data.
     */
    @DataProvider(name = "getRepositoryGuid")
    public Object[][] getRepositoryGuidDataProvider() {
        return new Object[][]{
            {"repository", true},
            {"unknown", false},
            {"eac7690f2ca05940e9239d5300037551", true},
            {"remote", false},
            {"88cd962fec779a3abafa95aad8ace74c", false},
            {"remote.repository", true},
            {"remote.unknown", false},
            {"remote.eac7690f2ca05940e9239d5300037551", true},
            {"88cd962fec779a3abafa95aad8ace74c.repository", true},
            {"88cd962fec779a3abafa95aad8ace74c.unknown", false},
            {"88cd962fec779a3abafa95aad8ace74c.eac7690f2ca05940e9239d5300037551", true},
            {"unknown.repository", false},
            {"unknown.unknown", false},
            {"unknown.eac7690f2ca05940e9239d5300037551", false},
            {"remote.repository.unknown", false}
        };
    }

    /**
     * Test.
     *
     * @param key Input repository key.
     * @param matches Whether supplied key is expected to generate a match.
     */
    @Test(dataProvider = "getRepositoryGuid")
    public void getRepositoryGuidTest(String key, boolean matches) {
        create(reachable(open()));
        storageManager.inTransaction(() -> {
            if (matches) {
                assertThat(remoteRepositoriesPool.getRepositoryGuid(key)).isEqualTo(REPOSITORY_GUID);
            } else {
                assertThrows(UnknownRepositoryException.class, () -> remoteRepositoriesPool.getRepositoryGuid(key));
            }
        });
    }

    /**
     * Test.
     */
    @Test
    public void tryGetRepositoryDefTest() {
        RepositoryInfo repositoryInfo = open();
        create(reachable(repositoryInfo));
        storageManager.inTransaction(() -> {
            RepositoryDef actual = remoteRepositoriesPool.tryGetRepositoryDef(REPOSITORY_GUID).get();
            RepositoryDef expected = new RepositoryDef(NODE_NAME + "." + REPOSITORY_NAME,
                                                       NODE_GUID,
                                                       NODE_URI.resolve("repositories/" + REPOSITORY_NAME).toString());

            assertThat(actual).isEqualTo(expected);
        });
    }

    /**
     * Test.
     */
    @Test
    public void tryGetRepositoryDefUnknownTest() {
        storageManager.inTransaction(() -> {
            assertThat(remoteRepositoriesPool.tryGetRepositoryDef(REPOSITORY_GUID).isPresent()).isFalse();
        });
    }

    /**
     * Data provider.
     *
     * @return getRepository test data.
     */
    @DataProvider(name = "getRepository")
    public Object[][] getRepositoryDataProvider() {
        return new Object[][]{
            {reachable(), UnknownRepositoryException.class},
            {unreachable(open()), UnreachableNodeException.class},
            {reachable(closed()), RepositoryClosedException.class},
            {reachable(open()), null}
        };
    }

    /**
     * Test.
     *
     * @param <T> Expected exception type.
     * @param remoteInfo Remote info to create as test set up.
     * @param clazz Expected exception class, if any. Null otherwise.
     */
    @Test(dataProvider = "getRepository")
    public <T extends NodeException> void getRepositoryTest(RemoteInfo remoteInfo, Class<T> clazz) {
        create(remoteInfo);
        if (clazz == null) {
            storageManager.inTransaction(() -> {
                assertThat(remoteRepositoriesPool.getRepository(REPOSITORY_GUID)).isNotNull();
            });
        } else {
            assertThrows(clazz, () -> {
                storageManager.inTransaction(() -> remoteRepositoriesPool.getRepository(REPOSITORY_GUID));
            });
        }
    }

    /**
     * Test.
     *
     * @param <T> Expected exception type.
     * @param remoteInfo Remote info to create as test set up.
     * @param clazz Expected exception class, if any. Null otherwise.
     */
    @Test(dataProvider = "getRepository")
    public <T extends NodeException> void tryGetRepositoryTest(RemoteInfo remoteInfo, Class<T> clazz) {
        create(remoteInfo);
        storageManager.inTransaction(() -> {
            assertThat(remoteRepositoriesPool.tryGetRepository(REPOSITORY_GUID).isPresent()).isEqualTo(clazz == null);
        });
    }

    /**
     * Test.
     */
    @Test
    public void getRepositoryCachedTest() {
        create(reachable(open()));
        storageManager.inTransaction(() -> {
            Repository repository = remoteRepositoriesPool.getRepository(REPOSITORY_GUID);
            assertThat(remoteRepositoriesPool.getRepository(REPOSITORY_GUID)).isEqualTo(repository);
        });
        verify(clientManager).getClient();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = RepositoryClosedException.class)
    public void tryCloseRepositoryTest() {
        create(reachable(open()));
        Repository repository = storageManager.inTransaction(() -> {
            return remoteRepositoriesPool.getRepository(REPOSITORY_GUID);
        });

        remoteRepositoriesPool.tryCloseRepository(REPOSITORY_GUID);
        repository.getDef();
    }

    /**
     * Test.
     */
    @Test
    public void tryCloseRepositoryUnknownTest() {
        remoteRepositoriesPool.tryCloseRepository(REPOSITORY_GUID);
    }

    private void create(RemoteInfo remoteInfo) {
        storageManager.inTransaction(() -> remotesDao.createRemoteInfo(remoteInfo));
    }

    private static RemoteInfo unreachable(RepositoryInfo... repositoryInfo) {
        return new RemoteInfo(nodeInfo(repositoryInfo), now());
    }

    private static RemoteInfo reachable(RepositoryInfo... repositoryInfo) {
        return new RemoteInfo(nodeInfo(repositoryInfo), NODE_URI, now());
    }

    private static NodeInfo nodeInfo(RepositoryInfo... repositoryInfo) {
        return new NodeInfo(new NodeDef(NODE_NAME, NODE_GUID, singletonList(NODE_URI)),
                            asList(repositoryInfo));
    }

    private static RepositoryInfo open() {
        return new RepositoryInfo(new RepositoryDef(REPOSITORY_NAME, REPOSITORY_GUID, REPOSITORY_PATH),
                                  new RepositoryStats(0, 0, 0, emptyMap()),
                                  new AgentInfo(0, 0, AgentState.WAITING),
                                  new AgentInfo(0, 0, AgentState.WAITING));
    }

    private static RepositoryInfo closed() {
        return new RepositoryInfo(new RepositoryDef(REPOSITORY_NAME, REPOSITORY_GUID, REPOSITORY_PATH));
    }

    private static <T extends NodeException> void assertThrows(Class<T> clazz, Runnable action) {
        try {
            action.run();
            throw new AssertionError("Expected an instance of " + clazz.getName() + " to be thrown");

        } catch (NodeException e) {
            if (clazz.isAssignableFrom(e.getClass())) {
                return;
            }
            throw e;
        }
    }
}
