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
    @DataProvider(name = "paramsDataProvider")
    public Object[][] paramsDataProvider() {
        return new Object[][]{
            {new Quit(), asList(), asList()},
            {new Quit(), asList("quit", "test"), asList("test")},
            {new Quit(), asList("quit", "one", "two"), asList("one", "two")},
            {new Get(), asList("get", "9d0a68c215bfcdc69b2a"), asList("9d0a68c215bfcdc69b2a")},
            {new AddRemote(), asList("add"), asList()},
            {new AddRemote(), asList("add", "remote"), asList()},
            {new AddRemote(), asList("add", "remote", "http://localhost:9400"), asList("http://localhost:9400")}
        };
    }

    /**
     * Test.
     *
     * @param command Command to test.
     * @param argList Command line arguments list to test.
     * @param params Expected parameters list.
     */
    @Test(dataProvider = "paramsDataProvider")
    void paramsTest(Command command, List<String> argList, List<String> params) {
        assertThat(command.params(argList)).as(command.name() + ", " + argList).isEqualTo(params);
    }

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
            {new DropRepository(), asList("primary")}
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
            {new DropRepository(), asList("repository", "primary")}
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
