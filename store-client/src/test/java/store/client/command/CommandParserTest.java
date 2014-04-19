package store.client.command;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.HttpClient;
import store.client.http.Session;
import store.common.RepositoryDef;

/**
 * Unit tests.
 */
public class CommandParserTest {

    private final CommandParser parser;

    {
        HttpClient client = mock(HttpClient.class);
        when(client.listRepositoryDefs()).thenReturn(asList(new RepositoryDef("primary", Paths.get("/repo/primary")),
                                                            new RepositoryDef("secondary", Paths.get("/repo/secondary"))));
        Display display = mock(Display.class);
        Session session = mock(Session.class);
        ClientConfig config = mock(ClientConfig.class);
        when(session.getClient()).thenReturn(client);

        parser = new CommandParser(display, session, config);
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
            {"drop", asList(" "), 4},
            {"drop ", asList("replication", "repository"), 5},
            {"drop repo", asList("repository"), 5},
            {"drop repository", asList(" "), 15},
            {"drop repository ", asList("primary", "secondary"), 16},
            {"drop repository p", asList("primary"), 16},
            {"drop repository primary", asList(" "), 23},
            {"drop repository primary ", asList(), 0},
            {"create repli", asList("replication"), 7}
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

        assertThat(actualCandidates).isEqualTo(expectedCandidates);
        assertThat(actualIndex).isEqualTo(expectedIndex);
    }
}
