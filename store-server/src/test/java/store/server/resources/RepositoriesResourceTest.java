package store.server.resources;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.SortedSet;
import javax.ws.rs.core.Application;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.joda.time.Instant;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import store.common.client.Client;
import store.common.client.Content;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentInfoTree;
import store.common.model.Event;
import store.common.model.Event.EventBuilder;
import store.common.model.IndexEntry;
import store.common.model.Operation;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import static store.server.TestUtil.LOREM_IPSUM;
import store.server.providers.MultipartReader;
import store.server.repository.Repository;
import store.server.service.RepositoriesService;

/**
 * Unit tests.
 */
public class RepositoriesResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesResourceTest.class);
    private final RepositoriesService repositoriesService = mock(RepositoriesService.class);
    private final Guid guid = Guid.random();
    private final Path path = Paths.get("/tmp/test");
    private final CommandResult result = CommandResult.noOp(1, LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
    private final RepositoryInfo repositoryInfo = new RepositoryInfo(new RepositoryDef("test", guid, path));
    private final Hash hash = LOREM_IPSUM.getHash();
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
    protected Application configure() {
        return new ResourceConfig()
                .register(RepositoriesResource.class)
                .register(MultipartReader.class)
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
            client.repositories().create(path);
        }
        verify(repositoriesService).createRepository(path);
    }

    /**
     * Test.
     */
    @Test
    public void addRepositoryTest() {
        try (Client client = newClient()) {
            client.repositories().add(path);
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
            assertThat(client.repositories().getInfo(guid)).isEqualTo(repositoryInfo);
        }
    }

    /**
     * Test.
     */
    @Test
    public void postInfoTest() {
        ContentInfo contentInfo = LOREM_IPSUM.getInfo();

        Repository repository = newRepositoryMock();
        when(repository.addContentInfo(contentInfo)).thenReturn(result);

        try (Client client = newClient()) {
            CommandResult actual = client.repositories().get(guid).addInfo(contentInfo);
            assertThat(actual).isEqualTo(result);
        }
    }

    /**
     * Test.
     *
     * @throws IOException Actually unexpected.
     */
    @Test
    public void addContentTest() throws IOException {
        long txId = 1;

        Repository repository = newRepositoryMock();
        when(repository.addContent(eq(txId), eq(hash), matches(LOREM_IPSUM.getBytes()))).thenReturn(result);

        try (Client client = newClient();
                InputStream input = LOREM_IPSUM.getInputStream()) {

            CommandResult actual = client.repositories().get(guid).addContent(txId, hash, input);
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
        when(repository.getContentInfoHead(hash)).thenReturn(singletonList(LOREM_IPSUM.getInfo()));
        when(repository.getContent(hash)).thenReturn(LOREM_IPSUM.getInputStream());

        try (Client client = newClient();
                Content content = client.repositories().get(guid).getContent(hash);
                InputStream expected = LOREM_IPSUM.getInputStream()) {

            assertThat(content.getFileName().get()).isEqualTo(LOREM_IPSUM.filename());
            assertThat(content.getInputStream()).hasContentEqualTo(expected);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getInfoTreeTest() {
        ContentInfoTree tree = LOREM_IPSUM.getTree();

        Repository repository = newRepositoryMock();
        when(repository.getContentInfoTree(hash)).thenReturn(tree);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).getInfoTree(hash)).isEqualTo(tree);
        }
    }

    /**
     * Test.
     */
    @Test
    public void getInfoHeadTest() {
        List<ContentInfo> head = singletonList(LOREM_IPSUM.getInfo());

        Repository repository = newRepositoryMock();
        when(repository.getContentInfoHead(hash)).thenReturn(head);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).getInfoHead(hash)).isEqualTo(head);
        }
    }

    /**
     * Test.
     */
    @Test
    public void historyTest() {
        boolean asc = true;
        List<Event> history = singletonList(
                new EventBuilder()
                .withOperation(Operation.CREATE)
                .withSeq(1)
                .withTimestamp(Instant.now())
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
    public void findInfoTest() {
        List<IndexEntry> entries = singletonList(new IndexEntry(hash, LOREM_IPSUM.getHead()));
        List<ContentInfo> infos = singletonList(LOREM_IPSUM.getInfo());

        Repository repository = newRepositoryMock();
        when(repository.find(query, first, size)).thenReturn(entries);
        when(repository.getContentInfoRevisions(hash, LOREM_IPSUM.getHead())).thenReturn(infos);

        try (Client client = newClient()) {
            assertThat(client.repositories().get(guid).findInfo(query, first, size)).isEqualTo(infos);
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
