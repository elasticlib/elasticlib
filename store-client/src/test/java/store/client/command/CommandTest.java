package store.client.command;

import static java.util.Arrays.asList;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class CommandTest {

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "isValidDataProvider")
    public Object[][] isValidDataProvider() {
        return new Object[][]{
            {new Quit(), asList()},
            {new Get(), asList("9d0a68c215bfcdc69b2a0f4852ef5d7aa6aa047e")},
            {new Set(), asList("repository", "primary")}
        };
    }

    /**
     * Test.
     *
     * @param command Command to test.
     * @param params Expected valid parameters list.
     */
    @Test(dataProvider = "isValidDataProvider")
    void isValidTest(Command command, List<String> params) {
        assertThat(command.isValid(params)).as(command.name() + ", " + params).isTrue();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "isInvalidDataProvider")
    public Object[][] isInvalidDataProvider() {
        return new Object[][]{
            {new Quit(), asList("repository")},
            {new Get(), asList("repository", "9d0a68c215bfcdc69b2a0f4852ef5d7aa6aa047e")},
            {new Set(), asList("primary")}
        };
    }

    /**
     * Test.
     *
     * @param command Command to test.
     * @param params Expected invalid parameters list.
     */
    @Test(dataProvider = "isInvalidDataProvider")
    void isInvalidTest(Command command, List<String> params) {
        assertThat(command.isValid(params)).as(command.name() + ", " + params).isFalse();
    }
}
