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
package org.elasticlib.node.resources;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.time.Instant.now;
import java.util.Arrays;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.SortedSet;
import static java.util.stream.Collectors.toList;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.client.Content;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.RangeNotSatisfiableException;
import org.elasticlib.common.exception.UnknownContentException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.Event.EventBuilder;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.value.Value;
import static org.elasticlib.node.TestUtil.LOREM_IPSUM;
import org.elasticlib.node.repository.Repository;
import org.elasticlib.node.service.RepositoriesService;
import static org.fest.assertions.api.Assertions.assertThat;
import org.fest.util.Files;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Integration tests on the repositories resource and the HTTP client. Service layer is mocked in these tests.
 */
public class RepositoriesResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesResourceTest.class);

    private final RepositoriesService repositoriesService = mock(RepositoriesService.class);
    private final Guid guid = Guid.random();
    private final Path path = Paths.get(Files.temporaryFolderPath(), "test");
    private final Hash hash = LOREM_IPSUM.getHash();
    private final long position = 0;
    private final StagingInfo stagingInfo = new StagingInfo(guid, hash, position);
    private final CommandResult result = CommandResult.noOp(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
    private final RepositoryInfo repositoryInfo = new RepositoryInfo(new RepositoryDef("test", guid, path.toString()));
    private final long offset = 100;
    private final long length = 200;
    private final int first = 0;
    private final int size = 20;
    private final String query = "lorem ipsum";

    /**
     * Constructor.
     */
    public RepositoriesResourceTest() {
        super(LOG);
        registerMocks(repositoriesService);
    }

    @Override
    protected Application testConfiguration() {
        return new ResourceConfig()
                .register(RepositoriesResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(repositoriesService).to(RepositoriesService.class);
                    }
                });
    }

    /**
     * Test.
     */
    @Test
    public void createRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().create(path.toString());
        }
        verify(repositoriesService).createRepository(path);
    }

    /**
     * Test.
     */
    @Test
    public void addRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().add(path.toString());
        }
        verify(repositoriesService).addRepository(path);
    }

    /**
     * Test.
     */
    @Test
    public void openRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().open(guid);
        }
        verify(repositoriesService).openRepository(guid.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void closeRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().close(guid);
        }
        verify(repositoriesService).closeRepository(guid.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void removeRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().remove(guid);
        }
        verify(repositoriesService).removeRepository(guid.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void deleteRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().delete(guid);
        }
        verify(repositoriesService).deleteRepository(guid.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void listRepositoriesTest() {
        List<RepositoryInfo> repositoryInfos = singletonList(repositoryInfo);

        when(repositoriesService.listRepositoryInfos()).thenReturn(repositoryInfos);
        try (Client client = newClient()) {
            assertThat(client.repositories().listInfos()).isEqualTo(repositoryInfos);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getRepositoryTest() {
        when(repositoriesService.getRepositoryInfo(guid.asHexadecimalString())).thenReturn(repositoryInfo);
        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).getInfo()).isEqualTo(repositoryInfo);
        }
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownRepositoryException.class)
    public void getRepositoryUnknownTest() {
        when(repositoriesService.getRepositoryInfo(guid.asHexadecimalString()))
                .thenThrow(new UnknownRepositoryException());

        try (Client client = newClient()) {
            client.repositories().get(guid).getInfo();
        }
    }

    /**
     * Test.
     */
    @Test
    public void stageContentTest() {
        Repository repository = newRepositoryMock();
        when(repository.stageContent(hash)).thenReturn(stagingInfo);

        try (Client client = newClient()) {
            StagingInfo actual = client.repositories().get(guid).stageContent(hash);
            assertThat(actual).isEqualTo(stagingInfo);
        }
    }

    /**
     * Test.
     *
     * @throws IOException Actually unexpected.
     */
    @Test
    public void writeContentTest() throws IOException {
        Repository repository = newRepositoryMock();
        when(repository.writeContent(eq(hash), eq(guid), matches(LOREM_IPSUM.getBytes()), eq(position)))
                .thenReturn(stagingInfo);

        try (Client client = newClient();
                InputStream input = LOREM_IPSUM.getInputStream()) {

            StagingInfo actual = client.repositories().get(guid).writeContent(hash, guid, input, position);
            assertThat(actual).isEqualTo(stagingInfo);
        }
    }

    /**
     * Test.
     *
     * @throws IOException Actually unexpected.
     */
    @Test(expectedExceptions = IOFailureException.class)
    public void writeContentWithIOFailureTest() throws IOException {
        Repository repository = newRepositoryMock();
        when(repository.writeContent(eq(hash), eq(guid), matches(LOREM_IPSUM.getBytes()), eq(position)))
                .thenThrow(new IOFailureException("test"));

        try (Client client = newClient();
                InputStream input = LOREM_IPSUM.getInputStream()) {

            client.repositories().get(guid).writeContent(hash, guid, input, position);
        }
    }

    /**
     * Test.
     */
    @Test
    public void unstageContentTest() {
        Repository repository = newRepositoryMock();
        try (Client client = newClient()) {
            client.repositories().get(guid).unstageContent(hash, guid);
        }
        verify(repository).unstageContent(hash, guid);
    }

    /**
     * Test.
     */
    @Test
    public void addRevisionTest() {
        Revision revision = LOREM_IPSUM.getRevision();

        Repository repository = newRepositoryMock();
        when(repository.addRevision(revision)).thenReturn(result);

        try (Client client = newClient()) {
            CommandResult actual = client.repositories().get(guid).addRevision(revision);
            assertThat(actual).isEqualTo(result);
        }
    }

    /**
     * Test.
     */
    @Test
    public void mergeTreeTest() {
        RevisionTree tree = LOREM_IPSUM.getTree();

        Repository repository = newRepositoryMock();
        when(repository.mergeTree(tree)).thenReturn(result);

        try (Client client = newClient()) {
            CommandResult actual = client.repositories().get(guid).mergeTree(tree);
            assertThat(actual).isEqualTo(result);
        }
    }

    /**
     * Test.
     */
    @Test
    public void deleteContentTest() {
        SortedSet<Hash> head = LOREM_IPSUM.getHead();

        Repository repository = newRepositoryMock();
        when(repository.deleteContent(hash, head)).thenReturn(result);

        try (Client client = newClient()) {
            CommandResult actual = client.repositories().get(guid).deleteContent(hash, head);
            assertThat(actual).isEqualTo(result);
        }
    }

    /**
     * Test.
     *
     * @throws IOException Actually unexpected.
     */
    @Test
    public void getContentTest() throws IOException {
        Repository repository = newRepositoryMock();
        when(repository.getTree(hash)).thenReturn(LOREM_IPSUM.getTree());
        when(repository.getContent(hash, 0, LOREM_IPSUM.getLength())).thenReturn(LOREM_IPSUM.getInputStream());

        try (Client client = newClient();
                Content content = client.repositories().get(guid).getContent(hash);
                InputStream expected = LOREM_IPSUM.getInputStream()) {

            assertThat(content.getFileName().get()).isEqualTo(LOREM_IPSUM.filename());
            assertThat(content.getContentType()).isEqualTo(MediaType.valueOf(LOREM_IPSUM.contentType()));
            assertThat(content.getInputStream()).hasContentEqualTo(expected);
        }
    }

    /**
     * Test.
     *
     * @throws IOException Actually unexpected.
     */
    @Test
    public void getPartialContentTest() throws IOException {
        Repository repository = newRepositoryMock();
        when(repository.getTree(hash)).thenReturn(LOREM_IPSUM.getTree());
        when(repository.getContent(hash, offset, length)).thenReturn(LOREM_IPSUM.getInputStream(offset, length));

        try (Client client = newClient();
                Content content = client.repositories().get(guid).getContent(hash, offset, length);
                InputStream expected = LOREM_IPSUM.getInputStream(offset, length)) {

            assertThat(content.getInputStream()).hasContentEqualTo(expected);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getDigestTest() {
        Digest expected = LOREM_IPSUM.getDigest();
        Repository repository = newRepositoryMock();
        when(repository.getDigest(hash)).thenReturn(expected);

        try (Client client = newClient()) {
            Digest actual = client.repositories().get(guid).getDigest(hash);
            assertThat(actual).isEqualTo(expected);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getPartialDigestTest() {
        Digest expected = LOREM_IPSUM.getDigest(offset, length);
        Repository repository = newRepositoryMock();
        when(repository.getDigest(hash, offset, length)).thenReturn(expected);

        try (Client client = newClient()) {
            Digest actual = client.repositories().get(guid).getDigest(hash, offset, length);
            assertThat(actual).isEqualTo(expected);
        }
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = RangeNotSatisfiableException.class)
    public void getContentWithUnsatisfiableRangeTest() {
        Repository repository = newRepositoryMock();
        when(repository.getTree(hash)).thenReturn(LOREM_IPSUM.getTree());

        try (Client client = newClient()) {
            client.repositories().get(guid).getContent(hash, 0, LOREM_IPSUM.getLength() + 1);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getContentInfoTest() {
        ContentInfo contentInfo = new ContentInfo(ContentState.PRESENT,
                                                  new Hash("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
                                                  0,
                                                  singletonList(LOREM_IPSUM.getRevision()));

        Repository repository = newRepositoryMock();
        when(repository.getContentInfo(hash)).thenReturn(contentInfo);

        try (Client client = newClient()) {
            ContentInfo actual = client.repositories().get(guid).getContentInfo(hash);
            assertThat(actual).isEqualTo(contentInfo);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getTreeTest() {
        RevisionTree tree = LOREM_IPSUM.getTree();

        Repository repository = newRepositoryMock();
        when(repository.getTree(hash)).thenReturn(tree);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).getTree(hash)).isEqualTo(tree);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getHeadTest() {
        List<Revision> head = singletonList(LOREM_IPSUM.getRevision());

        Repository repository = newRepositoryMock();
        when(repository.getHead(hash)).thenReturn(head);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).getHead(hash)).isEqualTo(head);
        }
    }

    /**
     * @return Test data.
     */
    @DataProvider(name = "getRevisionsDataProvider")
    public Object[][] getRevisionsDataProvider() {
        Revision base = LOREM_IPSUM.getRevision();
        Object updated = LOREM_IPSUM.add("test", Value.of("updated")).getRevision();
        return new Object[][]{
            {asList()},
            {asList(base)},
            {asList(base, updated)}
        };
    }

    /**
     * Test.
     *
     * @param expected Expected revisions.
     */
    @Test(dataProvider = "getRevisionsDataProvider")
    public void getRevisionsTest(List<Revision> expected) {
        List<Hash> revs = expected.stream()
                .map(x -> x.getRevision())
                .collect(toList());

        Repository repository = newRepositoryMock();
        when(repository.getRevisions(hash, revs)).thenReturn(expected);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).getRevisions(hash, revs)).isEqualTo(expected);
        }
    }

    /**
     * Test.
     */
    @Test
    public void historyTest() {
        boolean asc = true;
        List<Event> history = singletonList(new EventBuilder()
                .withOperation(Operation.CREATE)
                .withSeq(1)
                .withTimestamp(now())
                .withContent(hash)
                .withRevisions(LOREM_IPSUM.getHead())
                .build());

        Repository repository = newRepositoryMock();
        when(repository.history(asc, first, size)).thenReturn(history);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).history(asc, first, size)).isEqualTo(history);
        }
    }

    /**
     * Test.
     */
    @Test
    public void findTest() {
        List<IndexEntry> entries = singletonList(new IndexEntry(hash, LOREM_IPSUM.getHead()));

        Repository repository = newRepositoryMock();
        when(repository.find(query, first, size)).thenReturn(entries);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).find(query, first, size)).isEqualTo(entries);
        }
    }

    /**
     * Test.
     */
    @Test
    public void findRevisionsTest() {
        List<IndexEntry> entries = singletonList(new IndexEntry(hash, LOREM_IPSUM.getHead()));
        List<Revision> revisions = singletonList(LOREM_IPSUM.getRevision());

        Repository repository = newRepositoryMock();
        when(repository.find(query, first, size)).thenReturn(entries);
        when(repository.getRevisions(hash, LOREM_IPSUM.getHead())).thenReturn(revisions);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).findRevisions(query, first, size)).isEqualTo(revisions);
        }
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownContentException.class)
    public void findUnknownRevisionsTest() {
        List<IndexEntry> entries = singletonList(new IndexEntry(hash, LOREM_IPSUM.getHead()));

        Repository repository = newRepositoryMock();
        when(repository.find(query, first, size)).thenReturn(entries);
        when(repository.getRevisions(hash, LOREM_IPSUM.getHead())).thenThrow(new UnknownContentException());

        try (Client client = newClient()) {
            client.repositories().get(guid).findRevisions(query, first, size);
        }
    }

    private Repository newRepositoryMock() {
        Repository repository = mock(Repository.class);
        when(repositoriesService.getRepository(guid.asHexadecimalString())).thenReturn(repository);
        return repository;
    }

    private static InputStream matches(byte[] expected) {
        return argThat(new InputStreamMatcher(expected));
    }

    private static class InputStreamMatcher extends ArgumentMatcher<InputStream> {

        private final byte[] expected;

        public InputStreamMatcher(byte[] expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object argument) {
            if (!(argument instanceof InputStream)) {
                return false;
            }
            try {
                byte[] actual = new byte[expected.length];
                int actualLength = InputStream.class.cast(argument).read(actual, 0, expected.length + 1);

                return actualLength == expected.length && Arrays.equals(actual, expected);

            } catch (IOException e) {
                return false;
            }
        }
    }
}
