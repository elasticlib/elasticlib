package store.common;

import com.google.common.collect.ImmutableMap;
import static java.util.Arrays.asList;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.fest.assertions.api.IterableAssert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.value.Value;

/**
 * Unit tests.
 */
public class ContentInfoTreeTest {

    private final ContentInfo a0;
    private final ContentInfo a1;
    private final ContentInfo a2;
    private final ContentInfo a3;
    private final ContentInfo a4;
    private final ContentInfo b2;
    private final ContentInfo c3;
    private final ContentInfo d4;
    private final ContentInfo d5;

    {
        //     A5
        //     |  \
        //     |   \
        //     A4   \    D4
        //     | \   \  /
        //     |  \   C3
        //     A3 |  /
        //     |  B2
        //     A2 |
        //     |  /
        //     A1
        //     |
        //     A0
        //

        a0 = revision("a0",
                      ImmutableMap.of("msg", Value.of("good morning")));

        a1 = revision("a1",
                      ImmutableMap.of("msg", Value.of("hello")),
                      "a0");

        a2 = revision("a2",
                      ImmutableMap.of("bool", Value.of(false), "msg", Value.of("hello world")),
                      "a1");

        a3 = revision("a3",
                      ImmutableMap.of("bool", Value.of(true), "msg", Value.of("hello world")),
                      "a2");

        b2 = revision("b2",
                      ImmutableMap.of("answer", Value.of(42), "msg", Value.of("hello")),
                      "a1");

        c3 = revision("c3",
                      ImmutableMap.of("answer", Value.of(42), "msg", Value.of("hello you")),
                      "b2");

        a4 = revision("a4",
                      ImmutableMap.of("answer", Value.of(42), "bool", Value.of(true), "msg", Value.of("hello world")),
                      "a3", "b2");

        d4 = deleted("d4", "c3");
        d5 = deleted("d4", "a4", "c3");

    }

    private static ContentInfo revision(String rev, Map<String, Value> metadata, String... parents) {
        return builder(parents)
                .withMetadata(metadata)
                .build(new Hash(rev));
    }

    private static ContentInfo deleted(String rev, String... parents) {
        return builder(parents)
                .withDeleted(true)
                .build(new Hash(rev));
    }

    private static ContentInfoBuilder builder(String... parents) {
        ContentInfoBuilder builder = new ContentInfoBuilder()
                .withHash(new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"))
                .withLength(1024);

        for (String parent : parents) {
            builder.withParent(new Hash(parent));
        }
        return builder;
    }

    private static ContentInfoTree tree(ContentInfo... infos) {
        return new ContentInfoTreeBuilder()
                .addAll(asList(infos))
                .build();
    }

    private static ContentInfo[] array(ContentInfo... infos) {
        return infos;
    }

    private static Hash[] revisions(ContentInfo... infos) {
        Hash[] revisions = new Hash[infos.length];
        for (int i = 0; i < infos.length; i++) {
            revisions[i] = infos[i].getRev();
        }
        return revisions;
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "listTestDataProvider")
    public Object[][] listTestDataProvider() {
        return new Object[][]{
            new Object[]{tree(a0), array(a0)},
            new Object[]{tree(a3, b2), array(a3, b2)},
            new Object[]{tree(a1, a2, a3), array(a3, a2, a1)},
            new Object[]{tree(a0, a1, a2, a3), array(a3, a2, a1, a0)},
            new Object[]{tree(a0, a1, a2, a3, b2), array(a3, b2, a2, a1, a0)},
            new Object[]{tree(a0, a1, a2, a3, b2, a4), array(a4, a3, b2, a2, a1, a0)}
        };
    }

    /**
     * Test.
     *
     * @param tree Input tree
     * @param expected Expected output
     */
    @Test(dataProvider = "listTestDataProvider")
    public void listTest(ContentInfoTree tree, ContentInfo[] expected) {
        assertThat(tree.list()).containsExactly(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "headTestDataProvider")
    public Object[][] headTestDataProvider() {
        return new Object[][]{
            new Object[]{tree(a0), revisions(a0)},
            new Object[]{tree(a3, b2), revisions(a3, b2)},
            new Object[]{tree(a1, a2, a3), revisions(a3)},
            new Object[]{tree(a0, a1, a2, a3), revisions(a3)},
            new Object[]{tree(a0, a1, a2, a3, b2), revisions(a3, b2)},
            new Object[]{tree(a0, a1, a2, a3, b2, a4), revisions(a4)}
        };
    }

    /**
     * Test.
     *
     * @param tree Input tree
     * @param expected Expected output
     */
    @Test(dataProvider = "headTestDataProvider")
    public void headTest(ContentInfoTree tree, Hash[] expected) {
        assertThat(tree.head()).containsExactly(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "tailTestDataProvider")
    public Object[][] tailTestDataProvider() {
        return new Object[][]{
            new Object[]{tree(a0), revisions(a0)},
            new Object[]{tree(a3, b2), revisions(a3, b2)},
            new Object[]{tree(a1, a2, a3), revisions(a1)},
            new Object[]{tree(a0, a1, a2, a3), revisions(a0)},
            new Object[]{tree(a0, a1, a2, a3, b2), revisions(a0)},
            new Object[]{tree(a0, a1, a2, a3, b2, a4), revisions(a0)}
        };
    }

    /**
     * Test.
     *
     * @param tree Input tree
     * @param expected Expected output
     */
    @Test(dataProvider = "tailTestDataProvider")
    public void tailTest(ContentInfoTree tree, Hash[] expected) {
        assertThat(tree.tail()).containsExactly(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "unknownParentsTestDataProvider")
    public Object[][] unknownParentsTestDataProvider() {
        return new Object[][]{
            new Object[]{tree(a0), revisions()},
            new Object[]{tree(a3, b2), revisions(a1, a2)},
            new Object[]{tree(a1, a2, a3), revisions(a0)},
            new Object[]{tree(a0, a1, a2, a3), revisions()},
            new Object[]{tree(a0, a1, a2, a3, b2), revisions()},
            new Object[]{tree(a0, a1, a2, a3, b2, a4), revisions()}
        };
    }

    /**
     * Test.
     *
     * @param tree Input tree
     * @param expected Expected output
     */
    @Test(dataProvider = "unknownParentsTestDataProvider")
    public void unknownParentsTest(ContentInfoTree tree, Hash[] expected) {
        IterableAssert<Hash> assertion = assertThat(tree.unknownParents());
        if (expected.length == 0) {
            assertion.isEmpty();
        } else {
            assertion.containsExactly(expected);
        }
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "noMergeTestDataProvider")
    public Object[][] noMergeTestDataProvider() {
        return new Object[][]{
            new Object[]{tree(a0)},
            new Object[]{tree(a3, b2)},
            new Object[]{tree(a1, a2, a3)},
            new Object[]{tree(a0, a1, a2, a3)},
            new Object[]{tree(a0, a1, a2, a3, a4, b2)},
            new Object[]{tree(a0, a1, a2, a3, b2, c3)},
            new Object[]{tree(a0, a1, a2, a3, a4, b2, c3)},
            new Object[]{tree(a0, a1, a2, a3, a4, b2, c3, d4)},
            new Object[]{tree(a0, a1, a2, a3, a4, b2, c3, d4, d5)}
        };
    }

    /**
     * Test.
     *
     * @param tree Input tree
     */
    @Test(dataProvider = "noMergeTestDataProvider")
    public void noMergeTest(ContentInfoTree tree) {
        assertThat(tree.merge()).isEqualTo(tree);
    }

    /**
     * Test.
     */
    @Test
    public void simpleThreeWayMergeTest() {
        ContentInfoTree merge = tree(a0, a1, a2, a3, b2).merge();

        assertThat(merge.head()).hasSize(1);
        assertThat(merge.get(merge.head().first()).getMetadata()).isEqualTo(a4.getMetadata());
    }

    /**
     * Test.
     */
    @Test
    public void sameMetadataMergeTest() {
        ContentInfo a1Bis = revision("b1",
                                     ImmutableMap.of("msg", Value.of("hello")),
                                     "a0");

        ContentInfoTree merge = tree(a0, a1, a1Bis).merge();

        assertThat(merge.head()).hasSize(1);
        assertThat(merge.get(merge.head().first()).getMetadata()).isEqualTo(ImmutableMap.of("msg", Value.of("hello")));
    }

    /**
     * Test.
     */
    @Test
    public void noAncestorMergeTest() {
        ContentInfo rev1a = revision("1a", ImmutableMap.of("msg", Value.of("hello")));
        ContentInfo rev1b = revision("1b", ImmutableMap.of("int", Value.of(10)));

        ContentInfoTree merge = tree(rev1a, rev1b).merge();

        assertThat(merge.head()).hasSize(1);
        assertThat(merge.get(merge.head().first()).getMetadata())
                .isEqualTo(ImmutableMap.of("int", Value.of(10), "msg", Value.of("hello")));
    }

    /**
     * Test.
     */
    @Test
    public void crissCrossMergeTest() {
        ContentInfo rev0 = revision("00",
                                    ImmutableMap.of("int", Value.of(5), "msg", Value.of("good morning")));

        ContentInfo rev1a = revision("1a",
                                     ImmutableMap.of("int", Value.of(5), "msg", Value.of("hello")),
                                     "00");

        ContentInfo rev2a = revision("2a",
                                     ImmutableMap.of("int", Value.of(10), "msg", Value.of("hello world")),
                                     "1a", "1b");

        ContentInfo rev1b = revision("1b",
                                     ImmutableMap.of("int", Value.of(10), "msg", Value.of("good morning")),
                                     "00");

        ContentInfo rev2b = revision("2b",
                                     ImmutableMap.of("int", Value.of(20), "msg", Value.of("hello")),
                                     "1a", "1b");

        ContentInfoTree merge = tree(rev0, rev1a, rev2a, rev1b, rev2b).merge();

        assertThat(merge.head()).hasSize(1);
        assertThat(merge.get(merge.head().first()).getMetadata())
                .isEqualTo(ImmutableMap.of("int", Value.of(20), "msg", Value.of("hello world")));
    }
}
