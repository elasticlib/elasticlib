package org.elasticlib.node.components;

import com.google.common.base.Joiner;
import static com.sleepycat.je.EnvironmentConfig.LOG_MEM_ONLY;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import static org.elasticlib.node.config.NodeConfig.NODE_NAME;
import static org.elasticlib.node.config.NodeConfig.NODE_URIS;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.manager.ManagerModule;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Local repositories pool unit tests.
 */
@Test(singleThreaded = true)
public class LocalRepositoriesPoolTest {

    private static final String LOCAL_NODE_NAME = "node";
    private static final String REPOSITORY_NAME = "repository";
    private static final Guid NODE_GUID = new Guid("88cd962fec779a3abafa95aad8ace74c");
    private static final Guid REPOSITORY_GUID = new Guid("eac7690f2ca05940e9239d5300037551");
    private static final String REPOSITORY_PATH = "/tmp/repository";
    private static final String UNKNOWN = "unknown";

    private Path path;
    private ManagerModule managerModule;
    private RepositoriesDao repositoriesDao;
    private NodeGuidProvider nodeGuidProvider;
    private LocalRepositoriesPool localRepositoriesPool;

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
        Config config = config()
                .set(NODE_NAME, LOCAL_NODE_NAME)
                .set(NODE_URIS, "http://localhost:9400")
                .set(LOG_MEM_ONLY, "true");

        managerModule = new ManagerModule(path.resolve("home"), config);
        repositoriesDao = new RepositoriesDao(managerModule.getStorageManager());
        nodeGuidProvider = mock(NodeGuidProvider.class);
        localRepositoriesPool = new LocalRepositoriesPool(repositoriesDao,
                                                          new LocalRepositoriesFactory(config,
                                                                                       managerModule.getTaskManager(),
                                                                                       managerModule.getMessageManager()),
                                                          new NodeNameProvider(config),
                                                          nodeGuidProvider);

        when(nodeGuidProvider.guid()).thenReturn(NODE_GUID);

        managerModule.getStorageManager().inTransaction(localRepositoriesPool::start);
        managerModule.getStorageManager().inTransaction(localRepositoriesPool::start);
    }

    /**
     * Test tear-down.
     */
    @AfterMethod
    public void tearDown() {
        localRepositoriesPool.stop();
        managerModule.stop();
    }

    /**
     * Data provider.
     *
     * @return getRepositoryGuid test data.
     */
    @DataProvider(name = "tryGetRepositoryGuid")
    public Object[][] tryGetRepositoryGuidDataProvider() {
        return new Object[][]{
            {key(REPOSITORY_NAME), true},
            {key(UNKNOWN), false},
            {key(REPOSITORY_GUID), true},
            {key(LOCAL_NODE_NAME), false},
            {key(NODE_GUID), false},
            {key(LOCAL_NODE_NAME, REPOSITORY_NAME), true},
            {key(LOCAL_NODE_NAME, UNKNOWN), false},
            {key(LOCAL_NODE_NAME, REPOSITORY_GUID), true},
            {key(NODE_GUID, REPOSITORY_NAME), true},
            {key(NODE_GUID, UNKNOWN), false},
            {key(NODE_GUID, REPOSITORY_GUID), true},
            {key(UNKNOWN, REPOSITORY_NAME), false},
            {key(UNKNOWN, UNKNOWN), false},
            {key(UNKNOWN, REPOSITORY_GUID), false},
            {key(LOCAL_NODE_NAME, REPOSITORY_NAME, UNKNOWN), false}
        };
    }

    private static String key(Object... parts) {
        return Joiner.on(".").join(parts);
    }

    /**
     * Test.
     *
     * @param key Input repository key.
     * @param matches Whether supplied key is expected to generate a match.
     */
    @Test(dataProvider = "tryGetRepositoryGuid")
    public void tryGetRepositoryGuidTest(String key, boolean matches) {
        managerModule.getStorageManager().inTransaction(() -> {
            repositoriesDao.createRepositoryDef(new RepositoryDef(REPOSITORY_NAME, REPOSITORY_GUID, REPOSITORY_PATH));
        });
        managerModule.getStorageManager().inTransaction(() -> {
            if (matches) {
                assertThat(localRepositoriesPool.tryGetRepositoryGuid(key).get()).isEqualTo(REPOSITORY_GUID);
            } else {
                assertThat(localRepositoriesPool.tryGetRepositoryGuid(key).isPresent()).isFalse();
            }
        });
    }
}
