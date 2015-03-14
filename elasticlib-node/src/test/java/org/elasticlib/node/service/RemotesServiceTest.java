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
import java.time.Instant;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Optional;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.NodeAlreadyTrackedException;
import org.elasticlib.common.exception.SelfTrackingException;
import org.elasticlib.common.exception.UnknownNodeException;
import org.elasticlib.common.exception.UnreachableNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import static org.elasticlib.node.config.NodeConfig.REMOTES_CLEANUP_ENABLED;
import static org.elasticlib.node.config.NodeConfig.REMOTES_PING_ENABLED;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.manager.ManagerModule;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * remote nodes service integration tests.
 */
@Test(singleThreaded = true)
public class RemotesServiceTest {

    private static final URI TEST_URI = URI.create("http://localhost:9400");
    private static final Guid LOCAL_GUID = new Guid("eac7690f2ca05940e9239d5300037551");
    private static final NodeDef LOCAL_DEF = new NodeDef("local", LOCAL_GUID, emptyList());
    private static final RemoteInfo LOCAL_INFO = remoteInfo(LOCAL_DEF, TEST_URI);
    private static final Guid REMOTE_GUID = new Guid("da8d63a4a8bd8760a203b18a948fab75");
    private static final NodeDef REMOTE_DEF = new NodeDef("remote", REMOTE_GUID, emptyList());
    private static final RemoteInfo REMOTE_INFO = remoteInfo(REMOTE_DEF, TEST_URI);

    private Path path;

    private ManagerModule managerModule;
    private NodePingHandler nodePingHandler;
    private NodeService nodeService;
    private RemotesService remotesService;

    private static RemoteInfo remoteInfo(NodeDef def, URI transportUri) {
        return new RemoteInfo(new NodeInfo(def, emptyList()), transportUri, Instant.now());
    }

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
                .set(REMOTES_PING_ENABLED, false)
                .set(REMOTES_CLEANUP_ENABLED, false)
                .set(LOG_MEM_ONLY, "true");

        managerModule = new ManagerModule(path.resolve("home"), config);

        nodePingHandler = mock(NodePingHandler.class, CALLS_REAL_METHODS);
        doThrow(AssertionError.class).when(nodePingHandler).ping(any(URI.class));

        nodeService = mock(NodeService.class);
        when(nodeService.getGuid()).thenReturn(LOCAL_GUID);
        when(nodeService.getNodeDef()).thenReturn(LOCAL_DEF);

        remotesService = new RemotesService(config,
                                            null,
                                            managerModule.getStorageManager(),
                                            new RemotesDao(managerModule.getStorageManager()),
                                            nodeService,
                                            nodePingHandler);

        managerModule.start();
        remotesService.start();
    }

    /**
     * Stops services.
     */
    @AfterMethod
    public void stopServices() {
        remotesService.stop();
        managerModule.stop();
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteWithExpectedGuidTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.saveRemote(singletonList(TEST_URI), REMOTE_GUID);
        assertHasRemote(REMOTE_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteWithExpectedGuidTwiceTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.saveRemote(singletonList(TEST_URI), REMOTE_GUID);
        remotesService.saveRemote(singletonList(TEST_URI), REMOTE_GUID);
        assertHasRemote(REMOTE_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteWithExpectedGuidUnreachableTest() {
        whenPingReturnEmpty();
        remotesService.saveRemote(singletonList(TEST_URI), REMOTE_GUID);
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteWithUnexpectedGuidTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.saveRemote(singletonList(TEST_URI), new Guid("548bda2526d6537eb7a679d8d20b252e"));
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteWithExpectedGuidSelfTest() {
        remotesService.saveRemote(singletonList(TEST_URI), LOCAL_GUID);
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.saveRemote(TEST_URI);
        assertHasRemote(REMOTE_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteTwiceTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.saveRemote(TEST_URI);
        remotesService.saveRemote(TEST_URI);
        assertHasRemote(REMOTE_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteSelfTest() {
        whenPingReturn(LOCAL_INFO);
        remotesService.saveRemote(TEST_URI);
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test
    public void saveRemoteUnreachableTest() {
        whenPingReturnEmpty();
        remotesService.saveRemote(TEST_URI);
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test
    public void addRemoteTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
        assertHasRemote(REMOTE_INFO);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = NodeAlreadyTrackedException.class)
    public void addRemoteTwiceTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
        remotesService.addRemote(singletonList(TEST_URI));
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = SelfTrackingException.class)
    public void addRemoteSelfTest() {
        whenPingReturn(LOCAL_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnreachableNodeException.class)
    public void addRemoteUnreachableTest() {
        whenPingReturnEmpty();
        remotesService.addRemote(singletonList(TEST_URI));
    }

    /**
     * Test.
     */
    @Test
    public void removeRemoteByGuidTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
        remotesService.removeRemote(REMOTE_GUID.asHexadecimalString());
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test
    public void removeRemoteByNameTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
        remotesService.removeRemote(REMOTE_DEF.getName());
        assertHasNoRemote();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownNodeException.class)
    public void removeRemoteUnknownTest() {
        remotesService.removeRemote(REMOTE_GUID.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void pingRemotesTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));

        RemoteInfo updated = remoteInfo(new NodeDef("updated", REMOTE_GUID, emptyList()), TEST_URI);
        whenPingReturn(updated);
        remotesService.pingRemotes();
        assertHasRemote(updated);
    }

    /**
     * Test.
     */
    @Test
    public void pingRemotesUnreachableTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));

        whenPingReturnEmpty();
        remotesService.pingRemotes();

        List<RemoteInfo> updated = remotesService.listRemotes();
        assertThat(updated).hasSize(1);
        assertThat(updated.get(0).getGuid()).isEqualTo(REMOTE_GUID);
        assertThat(updated.get(0).isReachable()).isFalse();

        assertThat(remotesService.listReachableRemotes()).isEmpty();
    }

    /**
     * Test.
     */
    @Test
    public void cleanupRemotesTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
        remotesService.cleanupRemotes();
        assertHasRemote(REMOTE_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void cleanupRemotesUnreachableTest() {
        whenPingReturn(REMOTE_INFO);
        remotesService.addRemote(singletonList(TEST_URI));
        whenPingReturnEmpty();
        remotesService.pingRemotes();
        remotesService.cleanupRemotes();
        assertHasNoRemote();
    }

    private void whenPingReturn(RemoteInfo info) {
        doReturn(Optional.of(info)).when(nodePingHandler).ping(TEST_URI);
    }

    private void whenPingReturnEmpty() {
        doReturn(Optional.empty()).when(nodePingHandler).ping(TEST_URI);
    }

    private void assertHasRemote(RemoteInfo expected) {
        assertThat(remotesService.listRemotes()).containsExactly(expected);
    }

    private void assertHasNoRemote() {
        assertThat(remotesService.listRemotes()).isEmpty();
    }
}
