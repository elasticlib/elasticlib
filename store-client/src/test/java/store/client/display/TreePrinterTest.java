package store.client.display;

import com.google.common.base.Function;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.hash.Hash;
import store.common.model.Revision;
import store.common.model.Revision.RevisionBuilder;

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

    private static String tree(Revision... revisions) {
        TreePrinter printer = new TreePrinter(2, new Function<Revision, String>() {
            @Override
            public String apply(Revision rev) {
                return rev.getRevision().asHexadecimalString();
            }
        });
        for (Revision info : revisions) {
            printer.add(info);
        }
        return printer.print();
    }

    private static Revision revision(String rev, String... parents) {
        RevisionBuilder builder = new RevisionBuilder()
                .withContent(new Hash("ff"))
                .withLength(1024);

        for (String parent : parents) {
            builder.withParent(new Hash(parent));
        }

        return builder.build(new Hash(rev));
    }
}
