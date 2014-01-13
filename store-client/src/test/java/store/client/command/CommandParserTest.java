package store.client.command;

import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.client.Display;
import store.client.RestClient;
import store.client.Session;

/**
 * Unit tests.
 */
public class CommandParserTest {

    private final CommandParser parser;

    {
        RestClient restClient = mock(RestClient.class);
        when(restClient.listRepositories()).thenReturn(asList("primary", "secondary"));

        Display display = mock(Display.class);
        Session session = mock(Session.class);
        when(session.getRestClient()).thenReturn(restClient);

        parser = new CommandParser(display, session);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][]{
            {"se", asList("set"), 0},
            {"set", asList(" "), 3},
            {"set ", asList("repository"), 4},
            {"set repo", asList("repository"), 4},
            {"set repository", asList(" "), 14},
            {"set repository ", asList("primary", "secondary"), 15},
            {"set repository p", asList("primary"), 15},
            {"set repository primary", asList(" "), 22},
            {"set repository primary ", asList(), 0},
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
