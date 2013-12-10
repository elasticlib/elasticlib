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
        when(restClient.listVolumes()).thenReturn(asList("primary", "secondary"));

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
            {"se", 2, asList("set"), 0},
            {"set", 3, asList(" "), 3},
            {"set ", 4, asList("index", "volume"), 4},
            {"set vol", 7, asList("volume"), 4},
            {"set volume", 10, asList(" "), 10},
            {"set volume ", 11, asList("primary", "secondary"), 11},
            {"set volume p", 12, asList("primary"), 11},
            {"set volume primary", 18, asList(" "), 18},
            {"set volume primary ", 19, asList(), 0},
            {"create re", 9, asList("replication"), 7}
        };
    }

    /**
     * Test.
     *
     * @param buffer Buffer to complete.
     * @param cursor Cursor position in the buffer.
     * @param expectedCandidates Expected completion candidates.
     * @param expectedIndex Expected completion index.
     */
    @Test(dataProvider = "dataProvider")
    public void completeTest(String buffer, int cursor, List<CharSequence> expectedCandidates, int expectedIndex) {
        List<CharSequence> actualCandidates = new ArrayList<>();
        int actualIndex = parser.complete(buffer, cursor, actualCandidates);

        assertThat(actualCandidates).isEqualTo(expectedCandidates);
        assertThat(actualIndex).isEqualTo(expectedIndex);
    }
}
