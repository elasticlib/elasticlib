package org.elasticlib.node.service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.hash.Guid;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * node service integration tests.
 */
public class NodeServiceTest {

    private static final String NAME = "test";
    private static final String LOCALHOST = "http://localhost:9400";

    private Path path;
    private ManagerModule managerModule;
    private RepositoriesService repositoriesService;
    private NodeService nodeService;

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
        Config config = config()
                .set(NODE_NAME, NAME)
                .set(NODE_URIS, LOCALHOST);

        managerModule = new ManagerModule(path.resolve("home"), config);

        repositoriesService = new RepositoriesService(config,
                                                      managerModule.getTaskManager(),
                                                      managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      new RepositoriesDao(managerModule.getStorageManager()));

        nodeService = new NodeService(managerModule.getStorageManager(),
                                      new AttributesDao(managerModule.getStorageManager()),
                                      repositoriesService,
                                      new NodeNameProvider(config),
                                      new PublishUrisProvider(config));

        managerModule.start();
        repositoriesService.start();
        nodeService.start();
    }

    private void stop() {
        nodeService.stop();
        repositoriesService.stop();
        managerModule.stop();
    }

    /**
     * Test.
     */
    @Test
    public void getGuidTest() {
        assertThat(nodeService.getGuid()).isNotNull();
    }

    /**
     * Test.
     */
    @Test
    public void getGuidAfterRestartTest() {
        Guid expected = nodeService.getGuid();
        stop();
        start();

        assertThat(nodeService.getGuid()).isEqualTo(expected);
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
        return new NodeDef(NAME, nodeService.getGuid(), singletonList(URI.create(LOCALHOST)));
    }

    private NodeInfo expectedInfo() {
        return new NodeInfo(expectedDef(), emptyList());
    }
}
