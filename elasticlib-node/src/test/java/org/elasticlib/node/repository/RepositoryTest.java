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
package org.elasticlib.node.repository;

import com.google.common.collect.ImmutableMap;
import static com.google.common.collect.Iterables.getLast;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.util.Arrays.copyOfRange;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.ConflictException;
import org.elasticlib.common.exception.InvalidRepositoryPathException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.UnknownContentException;
import static org.elasticlib.common.metadata.Properties.Common.CONTENT_TYPE;
import static org.elasticlib.common.metadata.Properties.Common.FILE_NAME;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.RepositoryStats;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.StagingInfo;
import static org.elasticlib.common.util.IoUtil.copy;
import org.elasticlib.common.value.Value;
import org.elasticlib.node.TestContent;
import static org.elasticlib.node.TestUtil.LOREM_IPSUM;
import static org.elasticlib.node.TestUtil.UNKNOWN_HASH;
import static org.elasticlib.node.TestUtil.async;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import org.elasticlib.node.manager.ManagerModule;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Repository integration tests.
 */
public class RepositoryTest {

    private static final String CREATE_REPOSITORY = "createRepository";
    private static final String CREATE_REPOSITORY_CHECKS = "createRepositoryChecks";
    private static final String ADD_CONTENT = "addContent";
    private static final String ADD_CONTENT_CHECKS = "addContentChecks";
    private static final String UPDATE_CONTENT = "updateContent";
    private static final String UPDATE_CONTENT_CHECKS = "updateContentChecks";
    private static final String DELETE_CONTENT = "deleteContent";
    private static final String DELETE_CONTENT_CHECKS = "deleteContentChecks";
    private static final String CLOSE_REPOSITORY = "closeRepository";
    private static final String CLOSE_REPOSITORY_CHECKS = "closeRepositoryChecks";
    private static final String OPEN_REPOSITORY = "openRepository";
    private static final String OPEN_REPOSITORY_CHECKS = "openRepositoryChecks";
    private static final String REPOSITORY = "repository";
    private static final String DESCRIPTION = "description";
    private static final String TEST = "test";
    private static final String LOREM_IPSUM_QUERY = "Lorem ipsum";
    private static final TestContent UPDATED_LOREM_IPSUM = LOREM_IPSUM.add(DESCRIPTION, Value.of(TEST));
    private static final TestContent DELETED_LOREM_IPSUM = UPDATED_LOREM_IPSUM.delete();
    private Config config;
    private Path path;
    private Path repositoryPath;
    private ManagerModule managerModule;
    private Repository repository;
    private RepositoryDef def;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        config = config();
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        repositoryPath = path.resolve(REPOSITORY);
        managerModule = new ManagerModule(path.resolve("home"), config);

        managerModule.start();
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        if (repository != null) {
            repository.close();
        }
        managerModule.stop();
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY)
    public void createRepositoryTest() {
        repository = Repository.create(repositoryPath,
                                       config,
                                       managerModule.getTaskManager(),
                                       managerModule.getMessageManager());

        def = repository.getDef();
        assertThat(def.getName()).isEqualTo(REPOSITORY);
        assertThat(def.getPath()).isEqualTo(repositoryPath);

        RepositoryInfo info = repository.getInfo();
        assertThat(info.getDef()).isEqualTo(def);
        assertThat(info.isOpen()).isTrue();

        assertThat(repository.history(true, 0, 10)).isEmpty();

        async(() -> {
            assertUpToDate(repository.getInfo(),
                           new RepositoryStats(0, 0, 0, Collections.<String, Long>emptyMap()));
        });
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS,
          dependsOnGroups = CREATE_REPOSITORY,
          expectedExceptions = InvalidRepositoryPathException.class)
    public void createRepositoryAlreadyExistsTest() {
        Repository.create(repositoryPath,
                          config,
                          managerModule.getTaskManager(),
                          managerModule.getMessageManager());
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS,
          dependsOnGroups = CREATE_REPOSITORY,
          expectedExceptions = ConflictException.class)
    public void addContentWithUnknownParentTest() {
        repository.addRevision(UPDATED_LOREM_IPSUM.getRevision());
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT, dependsOnGroups = CREATE_REPOSITORY_CHECKS)
    public void addContentTest() throws IOException {
        ContentInfo contentInfo = repository.getContentInfo(LOREM_IPSUM.getHash());
        assertThat(contentInfo.getState()).isEqualTo(ContentState.ABSENT);

        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            StagingInfo stagingInfo = repository.stageContent(LOREM_IPSUM.getHash());
            assertThat(stagingInfo.getLength()).isZero();

            stagingInfo = repository.writeContent(LOREM_IPSUM.getHash(), stagingInfo.getSessionId(), inputStream, 0);
            assertThat(stagingInfo.getHash()).isEqualTo(LOREM_IPSUM.getHash());
        }

        CommandResult result = repository.addRevision(LOREM_IPSUM.getRevision());
        assertThat(result.getOperation()).isEqualTo(Operation.CREATE);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentTest() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                InputStream inputStream = repository.getContent(LOREM_IPSUM.getHash(), 0, Long.MAX_VALUE)) {
            copy(inputStream, outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(LOREM_IPSUM.getBytes());
        }
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentStartingRangeTest() throws IOException {
        getContentRangeTest(0, 100);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentMiddleRangeTest() throws IOException {
        getContentRangeTest(100, 100);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentPastEndRangeTest() throws IOException {
        getContentRangeTest(LOREM_IPSUM.getLength() + 1, 100, new byte[0]);
    }

    private void getContentRangeTest(long offset, long length) throws IOException {
        getContentRangeTest(offset, length, copyOfRange(LOREM_IPSUM.getBytes(), (int) offset, (int) (offset + length)));
    }

    private void getContentRangeTest(long offset, long length, byte[] expected) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                InputStream inputStream = repository.getContent(LOREM_IPSUM.getHash(), offset, length)) {
            copy(inputStream, outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(expected);
        }
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS,
          dependsOnGroups = ADD_CONTENT,
          expectedExceptions = UnknownContentException.class)
    public void getContentWithUnknownHashTest() {
        repository.getContent(UNKNOWN_HASH, 0, Long.MAX_VALUE);
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getHeadTest() {
        List<Revision> head = repository.getHead(LOREM_IPSUM.getHash());
        assertThat(head).containsExactly(LOREM_IPSUM.getRevision());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getTreeTest() {
        assertThat(repository.getTree(LOREM_IPSUM.getHash())).isEqualTo(LOREM_IPSUM.getTree());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS,
          dependsOnGroups = ADD_CONTENT,
          expectedExceptions = UnknownContentException.class)
    public void getTreeWithUnknownHashTest() {
        repository.getTree(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void historyTest() {
        List<Event> events = repository.history(true, 0, 10);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getOperation()).isEqualTo(Operation.CREATE);
        assertThat(events.get(0).getContent()).isEqualTo(LOREM_IPSUM.getHash());
        assertThat(events.get(0).getRevisions()).isEqualTo(LOREM_IPSUM.getHead());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void findTest() {
        async(() -> {
            IndexEntry expected = new IndexEntry(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
            assertThat(repository.find(LOREM_IPSUM_QUERY, 0, 10)).containsExactly(expected);
        });
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getInfoTest() {
        async(() -> {
            assertUpToDate(repository.getInfo(),
                           new RepositoryStats(1, 0, 0, ImmutableMap.of(FILE_NAME.key(), 1L,
                                                                        CONTENT_TYPE.key(), 1L)));
        });
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT, dependsOnGroups = ADD_CONTENT_CHECKS)
    public void updateTest() {
        CommandResult result = repository.addRevision(UPDATED_LOREM_IPSUM.getRevision());
        assertThat(result.getOperation()).isEqualTo(Operation.UPDATE);
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS,
          dependsOnGroups = UPDATE_CONTENT,
          expectedExceptions = ConflictException.class)
    public void updateWithConflictTest() {
        repository.addRevision(LOREM_IPSUM.add(DESCRIPTION, Value.of("conflict")).getRevision());
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void updateAlreadyUpdatedTest() {
        CommandResult result = repository.addRevision(UPDATED_LOREM_IPSUM.getRevision());
        assertThat(result.isNoOp()).isTrue();
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void getTreeAfterUpdateTest() {
        assertThat(repository.getTree(LOREM_IPSUM.getHash())).isEqualTo(UPDATED_LOREM_IPSUM.getTree());
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void historyAfterUpdateTest() {
        List<Event> events = repository.history(true, 0, 10);
        assertThat(events).hasSize(2);

        Event latest = getLast(events);
        assertThat(latest.getOperation()).isEqualTo(Operation.UPDATE);
        assertThat(latest.getContent()).isEqualTo(UPDATED_LOREM_IPSUM.getHash());
        assertThat(latest.getRevisions()).isEqualTo(UPDATED_LOREM_IPSUM.getHead());
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void findAfterUpdateTest() {
        async(() -> {
            IndexEntry expected = new IndexEntry(UPDATED_LOREM_IPSUM.getHash(), UPDATED_LOREM_IPSUM.getHead());
            assertThat(repository.find(DESCRIPTION + ":" + TEST, 0, 10)).containsExactly(expected);
        });
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void getInfoAfterUpdateTest() {
        async(() -> {
            assertUpToDate(repository.getInfo(),
                           new RepositoryStats(1, 1, 0, ImmutableMap.of(FILE_NAME.key(), 1L,
                                                                        CONTENT_TYPE.key(), 1L,
                                                                        DESCRIPTION, 1L)));
        });
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT, dependsOnGroups = UPDATE_CONTENT_CHECKS)
    public void deleteTest() {
        CommandResult result = repository.deleteContent(LOREM_IPSUM.getHash(), UPDATED_LOREM_IPSUM.getHead());
        assertThat(result.getOperation()).isEqualTo(Operation.DELETE);
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void deleteAlreadyDeletedTest() {
        CommandResult result = repository.deleteContent(LOREM_IPSUM.getHash(), DELETED_LOREM_IPSUM.getHead());
        assertThat(result.isNoOp()).isTrue();
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS,
          dependsOnGroups = DELETE_CONTENT,
          expectedExceptions = UnknownContentException.class)
    public void deleteUnknownHashTest() {
        repository.deleteContent(UNKNOWN_HASH, new TreeSet<>());
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void getTreeAfterDeleteTest() {
        assertThat(repository.getTree(LOREM_IPSUM.getHash())).isEqualTo(DELETED_LOREM_IPSUM.getTree());
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void historyAfterDeleteTest() {
        List<Event> events = repository.history(true, 0, 10);
        assertThat(events).hasSize(3);

        Event latest = getLast(events);
        assertThat(latest.getOperation()).isEqualTo(Operation.DELETE);
        assertThat(latest.getContent()).isEqualTo(DELETED_LOREM_IPSUM.getHash());
        assertThat(latest.getRevisions()).isEqualTo(DELETED_LOREM_IPSUM.getHead());
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void findAfterDeleteTest() {
        async(() -> {
            assertThat(repository.find(LOREM_IPSUM_QUERY, 0, 10)).isEmpty();
        });
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void getInfoAfterDeleteTest() {
        async(() -> {
            assertUpToDate(repository.getInfo(),
                           new RepositoryStats(1, 1, 1, Collections.<String, Long>emptyMap()));
        });
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY, dependsOnGroups = DELETE_CONTENT_CHECKS)
    public void closeRepositoryTest() {
        repository.close();
        assertThat(repository.getInfo().isOpen()).isFalse();
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY_CHECKS,
          dependsOnGroups = CLOSE_REPOSITORY,
          expectedExceptions = RepositoryClosedException.class)
    public void getTreeAfterCloseTest() {
        repository.getTree(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY, dependsOnGroups = CLOSE_REPOSITORY_CHECKS)
    public void openRepositoryTest() {
        repository = Repository.open(repositoryPath,
                                     config,
                                     managerModule.getTaskManager(),
                                     managerModule.getMessageManager());

        assertThat(repository.getInfo().isOpen()).isTrue();

        // Checks repository attributes have been properly saved / loaded.
        assertThat(repository.getDef()).isEqualTo(def);
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS,
          dependsOnGroups = OPEN_REPOSITORY,
          expectedExceptions = InvalidRepositoryPathException.class)
    public void openRepositoryUnknownTest() {
        repository = Repository.open(path.resolve("unknown"),
                                     config,
                                     managerModule.getTaskManager(),
                                     managerModule.getMessageManager());
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS, dependsOnGroups = OPEN_REPOSITORY)
    public void historyAfterOpenTest() {
        historyAfterDeleteTest();
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS, dependsOnGroups = OPEN_REPOSITORY)
    public void getInfoAfterOpenTest() {
        // Actually, we want to check state has not changed.
        getInfoAfterDeleteTest();
    }

    private static void assertUpToDate(RepositoryInfo info, RepositoryStats stats) {
        assertDone(info.getIndexingInfo());
        assertDone(info.getStatsInfo());
        assertThat(info.getStats()).isEqualTo(stats);
    }

    private static void assertDone(AgentInfo info) {
        assertThat(info.getState()).isEqualTo(AgentState.WAITING);
        assertThat(info.getCurSeq()).isEqualTo(info.getMaxSeq());
    }
}
