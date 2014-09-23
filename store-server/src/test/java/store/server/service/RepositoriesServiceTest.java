package store.server.service;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.config.Config;
import store.common.hash.Guid;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import static store.server.TestUtil.config;
import static store.server.TestUtil.recursiveDelete;
import store.server.dao.RepositoriesDao;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.RepositoryClosedException;
import store.server.exception.UnknownRepositoryException;
import store.server.manager.ManagerModule;
import store.server.repository.Repository;

/**
 * Unit tests.
 */
public class RepositoriesServiceTest {

    private static final String CREATE_REPOSITORY = "createRepository";
    private static final String CREATE_REPOSITORY_CHECKS = "createRepositoryChecks";
    private static final String CLOSE_REPOSITORY = "closeRepository";
    private static final String CLOSE_REPOSITORY_CHECKS = "closeRepositoryChecks";
    private static final String OPEN_REPOSITORY = "openRepository";
    private static final String OPEN_REPOSITORY_CHECKS = "openRepositoryChecks";
    private static final String REMOVE_REPOSITORY = "removeRepository";
    private static final String REMOVE_REPOSITORY_CHECKS = "removeRepositoryChecks";
    private static final String ADD_REPOSITORY = "addRepository";
    private static final String ADD_REPOSITORY_CHECKS = "addRepositoryChecks";
    private static final String DELETE_REPOSITORY = "deleteRepository";
    private static final String DELETE_REPOSITORY_CHECKS = "deleteRepositoryChecks";
    private static final String REPOSITORY = "repository";
    private Path path;
    private Path repositoryPath;
    private ManagerModule managerModule;
    private RepositoriesService repositoriesService;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        Config config = config();
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        repositoryPath = path.resolve(REPOSITORY);
        managerModule = new ManagerModule(path.resolve("home"), config);
        repositoriesService = new RepositoriesService(config,
                                                      managerModule.getTaskManager(),
                                                      managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      new RepositoriesDao(managerModule.getStorageManager()));

        managerModule.start();
        repositoriesService.start();
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        repositoriesService.stop();
        managerModule.stop();
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY)
    public void createRepositoryTest() {
        repositoriesService.createRepository(repositoryPath);
        assertOpen(repositoryInfo());
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS,
          dependsOnGroups = CREATE_REPOSITORY,
          expectedExceptions = RepositoryAlreadyExistsException.class)
    public void createRepositoryAlreadyExistsTest() {
        repositoriesService.createRepository(repositoryPath);
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS, dependsOnGroups = CREATE_REPOSITORY)
    public void getRepositoryInfoTest() {
        assertOpen(repositoriesService.getRepositoryInfo(REPOSITORY));
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS, dependsOnGroups = CREATE_REPOSITORY)
    public void getRepositoryByNameTest() {
        Repository repository = repositoriesService.getRepository(REPOSITORY);
        assertOpen(repository.getInfo());
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS, dependsOnGroups = CREATE_REPOSITORY)
    public void getRepositoryByGuidTest() {
        Repository repository = repositoriesService.getRepository(guid().asHexadecimalString());
        assertOpen(repository.getInfo());
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS,
          dependsOnGroups = CREATE_REPOSITORY,
          expectedExceptions = UnknownRepositoryException.class)
    public void getRepositoryUnknownTest() {
        repositoriesService.getRepository("unknown");
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS, dependsOnGroups = CREATE_REPOSITORY)
    public void tryGetRepositoryTest() {
        Optional<Repository> repository = repositoriesService.tryGetRepository(guid());
        assertThat(repository.isPresent()).isTrue();
        assertOpen(repository.get().getInfo());
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY, dependsOnGroups = CREATE_REPOSITORY_CHECKS)
    public void closeRepositoryTest() {
        repositoriesService.closeRepository(REPOSITORY);
        assertClosed(repositoryInfo());
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY_CHECKS, dependsOnGroups = CLOSE_REPOSITORY)
    public void closeRepositoryIsIndempotentTest() {
        repositoriesService.closeRepository(REPOSITORY);
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY_CHECKS, dependsOnGroups = CLOSE_REPOSITORY)
    public void getRepositoryInfoAfterCloseTest() {
        assertClosed(repositoriesService.getRepositoryInfo(REPOSITORY));
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY_CHECKS,
          dependsOnGroups = CLOSE_REPOSITORY,
          expectedExceptions = RepositoryClosedException.class)
    public void getRepositoryAfterCloseTest() {
        repositoriesService.getRepository(REPOSITORY);
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY_CHECKS, dependsOnGroups = CLOSE_REPOSITORY)
    public void tryGetRepositoryAfterCloseTest() {
        Optional<Repository> repository = repositoriesService.tryGetRepository(guid());
        assertThat(repository.isPresent()).isFalse();
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY, dependsOnGroups = CLOSE_REPOSITORY_CHECKS)
    public void openRepositoryTest() {
        repositoriesService.openRepository(REPOSITORY);
        assertOpen(repositoryInfo());
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS, dependsOnGroups = OPEN_REPOSITORY)
    public void openRepositoryIsIndempotentTest() {
        repositoriesService.openRepository(REPOSITORY);
    }

    /**
     * Test.
     */
    @Test(groups = REMOVE_REPOSITORY, dependsOnGroups = OPEN_REPOSITORY)
    public void removeRepositoryTest() {
        repositoriesService.removeRepository(REPOSITORY);
        assertThat(repositoriesService.listRepositoryInfos()).isEmpty();
    }

    /**
     * Test.
     */
    @Test(groups = REMOVE_REPOSITORY_CHECKS,
          dependsOnGroups = REMOVE_REPOSITORY,
          expectedExceptions = UnknownRepositoryException.class)
    public void removeRepositoryNotExistsTest() {
        repositoriesService.removeRepository(REPOSITORY);
    }

    /**
     * Test.
     */
    @Test(groups = ADD_REPOSITORY, dependsOnGroups = REMOVE_REPOSITORY_CHECKS)
    public void addRepositoryTest() {
        repositoriesService.addRepository(repositoryPath);
        assertOpen(repositoryInfo());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_REPOSITORY_CHECKS,
          dependsOnGroups = ADD_REPOSITORY,
          expectedExceptions = RepositoryAlreadyExistsException.class)
    public void addRepositoryAlreadyExistsTest() {
        repositoriesService.addRepository(repositoryPath);
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_REPOSITORY, dependsOnGroups = ADD_REPOSITORY_CHECKS)
    public void deleteRepositoryTest() {
        repositoriesService.deleteRepository(REPOSITORY);
        assertThat(repositoriesService.listRepositoryInfos()).isEmpty();
        assertThat(Files.exists(repositoryPath)).isFalse();
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_REPOSITORY_CHECKS,
          dependsOnGroups = DELETE_REPOSITORY,
          expectedExceptions = UnknownRepositoryException.class)
    public void deleteRepositoryNotExistsTest() {
        repositoriesService.deleteRepository(REPOSITORY);
    }

    private Guid guid() {
        return repositoryInfo().getDef().getGuid();
    }

    private RepositoryInfo repositoryInfo() {
        List<RepositoryInfo> infos = repositoriesService.listRepositoryInfos();
        assertThat(infos).hasSize(1);
        return infos.get(0);
    }

    private void assertOpen(RepositoryInfo info) {
        assertThat(info.isOpen()).isTrue();
        check(info.getDef());
    }

    private void assertClosed(RepositoryInfo info) {
        assertThat(info.isOpen()).isFalse();
        check(info.getDef());
    }

    private void check(RepositoryDef def) {
        assertThat(def.getName()).isEqualTo(REPOSITORY);
        assertThat(def.getPath()).isEqualTo(repositoryPath);
    }
}
