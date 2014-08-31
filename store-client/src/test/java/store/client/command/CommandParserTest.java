package store.client.command;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.client.config.ClientConfig;
import store.client.discovery.DiscoveryClient;
import store.client.display.Display;
import store.client.http.HttpClient;
import store.client.http.Session;
import store.common.RepositoryDef;
import store.common.RepositoryInfo;
import store.common.hash.Guid;

/**
 * Unit tests.
 */
public class CommandParserTest {

    private final CommandParser parser;

    {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.listRepositoryInfos())
                .thenReturn(asList(new RepositoryInfo(new RepositoryDef("primary",
                                                                        new Guid("8d5f3c77e94a0cad3a32340d342135f4"),
                                                                        Paths.get("/repo/primary"))),
                                   new RepositoryInfo(new RepositoryDef("secondary",
                                                                        new Guid("0d99dd9895a2a1c485e0c75f79f92cc1"),
                                                                        Paths.get("/repo/secondary")))));
        Display display = mock(Display.class);
        Session session = mock(Session.class);
        DiscoveryClient discoveryClient = mock(DiscoveryClient.class);
        ClientConfig config = mock(ClientConfig.class);
        when(session.getClient()).thenReturn(httpClient);

        parser = new CommandParser(display, session, config, discoveryClient);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][]{
            {"dr", asList("drop"), 0},
            {"drop", asList("drop"), 0},
            {"drop ", asList("replication", "repository"), 5},
            {"drop repo", asList("repository"), 5},
            {"drop repository", asList("repository"), 5},
            {"drop repository ", asList("0d99dd9895a2a1c485e0c75f79f92cc1", "8d5f3c77e94a0cad3a32340d342135f4",
                                        "primary", "secondary"), 16},
            {"drop repository p", asList("primary"), 16},
            {"drop repository primary", asList("primary"), 16},
            {"drop repository primary ", asList(), 24},
            {"create repli", asList("replication"), 7},
            {"ad", asList("add"), 0},
            {"add ", asList("remote", "repository"), 4},
            {"add re", asList("remote", "repository"), 4},
            {"add rem", asList("remote"), 4}
        };
    }

    /**
     * Test.
     *
     * @param buffer Buffer to complete.
     * @param expectedCandidates Expected completion candidates.
     * @param expectedIndex Expected completion index.
     */
    @Test(dataProvider = "dataProvider")
    public void completeTest(String buffer, List<CharSequence> expectedCandidates, int expectedIndex) {
        List<CharSequence> actualCandidates = new ArrayList<>();
        int actualIndex = parser.complete(buffer, buffer.length(), actualCandidates);

        assertThat(actualCandidates).as(buffer).isEqualTo(expectedCandidates);
        assertThat(actualIndex).as(buffer).isEqualTo(expectedIndex);
    }
}
