package org.elasticlib.console.tokenizing;

import static java.util.Arrays.asList;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class TokenizingTest {

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "argListDataProvider")
    public Object[][] argListDataProvider() {
        return new Object[][]{
            {"", asList()},
            {"pwd", asList("pwd")},
            {"!ls", asList("!", "ls")},
            {"ls!", asList("ls!")},
            {"ls  !", asList("ls", "!")},
            {"'!ls'", asList("!ls")},
            {"cd 'some where'", asList("cd", "some where")},
            {"cd 'some ''where'", asList("cd", "some 'where")},
            {"cd '", asList("cd", "")},
            {"cd ''", asList("cd", "")},
            {"cd '''", asList("cd", "'")},
            {"cd ''''", asList("cd", "'")},
            {"cd''", asList("cd", "")},
            {"cd''test", asList("cd", "", "test")},
            {"cd'' test", asList("cd", "", "test")},
            {"cd\nnl", asList("cd", "nl")},
            {"cd \r nl", asList("cd", "nl")},
            {"cd '\r nl'", asList("cd", "\r nl")},
            {"cd\r\nnl", asList("cd", "nl")},
            {"cd\n\rnl", asList("cd", "nl")},
            {"cd\n\n\rnl\n", asList("cd", "nl")}
        };
    }

    /**
     * Test.
     *
     * @param buffer Command line buffer.
     * @param expected Expected argument list.
     */
    @Test(dataProvider = "argListDataProvider")
    public void argListTest(String buffer, List<String> expected) {
        List<String> actual = Tokenizing.argList(buffer);
        assertThat(actual).as(buffer).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "lastArgumentPositionDataProvider")
    public Object[][] lastArgumentPositionDataProvider() {
        return new Object[][]{
            {"", 0},
            {"pwd", 0},
            {"ls  !", 4},
            {"'!ls'", 0},
            {"cd some", 3},
            {"cd  some", 4},
            {"cd 'some where'", 3},
            {"cd 'some where", 3},
            {"cd  'some where", 4},
            {"cd 'some ''where'", 3},
            {"cd 'some ''where", 3},
            {"cd '", 3},
            {"cd ''", 3},
            {"cd ''''", 3},
            {"cd''", 2},
            {"cd''test", 4},
            {"cd'' test", 5}
        };
    }

    /**
     * Test.
     *
     * @param buffer Command line buffer.
     * @param expected Expected result.
     */
    @Test(dataProvider = "lastArgumentPositionDataProvider")
    public void lastArgumentPositionTest(String buffer, int expected) {
        int actual = Tokenizing.lastArgumentPosition(buffer);
        assertThat(actual).as(buffer).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "isCompleteDataProvider")
    public Object[][] isCompleteDataProvider() {
        return new Object[][]{
            {"", true},
            {"pwd", true},
            {"ls  !", true},
            {"'!ls'", true},
            {"cd 'some where'", true},
            {"cd 'some where", false},
            {"cd 'some ''where'", true},
            {"cd '''some ''where'", true},
            {"cd '' 'some ''where'", true},
            {"cd 'some ''where", false},
            {"cd '", false},
            {"cd ''", true},
            {"cd ''''", true},
            {"cd''", true},
            {"cd''test", true},
            {"cd'' test", true}
        };
    }

    /**
     * Test.
     *
     * @param buffer Command line buffer.
     * @param expected Expected result.
     */
    @Test(dataProvider = "isCompleteDataProvider")
    public void isCompleteTest(String buffer, boolean expected) {
        boolean actual = Tokenizing.isComplete(buffer);
        assertThat(actual).as(buffer).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "escapeDataProvider")
    public Object[][] escapeDataProvider() {
        return new Object[][]{
            {"", ""},
            {"some", "some"},
            {"'some", "'some'"},
            {"'some'", "'some'"},
            {"some where", "'some where'"},
            {"'some where", "'some where'"},
            {"'some where'", "'some where'"},
            {"'some ' where", "'some '' where'"},
            {"'some '' where", "'some '' where'"},
            {"'some ''' where", "'some '''' where'"},
            {"'some ' where'", "'some '' where'"},
            {"'some '' where'", "'some '' where'"},
            {"'some ''' where'", "'some '''' where'"}
        };
    }

    /**
     * Test.
     *
     * @param argument Command line argument.
     * @param expected Expected result.
     */
    @Test(dataProvider = "escapeDataProvider")
    public void escapeTest(String argument, String expected) {
        String actual = Tokenizing.escape(argument);
        assertThat(actual).as(argument).isEqualTo(expected);
    }
}
