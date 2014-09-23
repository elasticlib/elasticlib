package store.client.display;

import com.google.common.base.Function;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.hash.Hash;
import store.common.model.ContentInfo;
import store.common.model.ContentInfo.ContentInfoBuilder;

/**
 * Unit tests.
 */
public class TreePrinterTest {

    /**
     * Test.
     */
    @Test
    public void printSimpleTest() {
        String expected = text("*  a2",
                               "*  a1",
                               "*  a0");

        String actual = tree(revision("a2", "a1"),
                             revision("a1", "a0"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printMergingTest() {
        String expected = text("*    a2",
                               "|\\   ",
                               "| *  a1",
                               "*  a0");

        String actual = tree(revision("a2", "a1", "a0"),
                             revision("a1"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printTripleMergingTest() {
        String expected = text("*-.    a3",
                               "|\\ \\   ",
                               "| | *  a2",
                               "| *  a1",
                               "*  a0");

        String actual = tree(revision("a3", "a2", "a1", "a0"),
                             revision("a2"),
                             revision("a1"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printBranchingTest() {
        String expected = text("*  a2",
                               "| *  a1",
                               "|/",
                               "*  a0");

        String actual = tree(revision("a2", "a0"),
                             revision("a1", "a0"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printTripleBranchingTest() {
        String expected = text("*  a3",
                               "| *  a2",
                               "|/",
                               "| *  a1",
                               "|/",
                               "*  a0");

        String actual = tree(revision("a3", "a0"),
                             revision("a2", "a0"),
                             revision("a1", "a0"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    private static String text(String... lines) {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append(System.lineSeparator());
        }
        return builder.toString();
    }

    private static String tree(ContentInfo... infos) {
        TreePrinter printer = new TreePrinter(2, new Function<ContentInfo, String>() {
            @Override
            public String apply(ContentInfo info) {
                return info.getRevision().asHexadecimalString();
            }
        });
        for (ContentInfo info : infos) {
            printer.add(info);
        }
        return printer.print();
    }

    private static ContentInfo revision(String rev, String... parents) {
        ContentInfoBuilder builder = new ContentInfoBuilder()
                .withContent(new Hash("ff"))
                .withLength(1024);

        for (String parent : parents) {
            builder.withParent(new Hash(parent));
        }

        return builder.build(new Hash(rev));
    }
}
