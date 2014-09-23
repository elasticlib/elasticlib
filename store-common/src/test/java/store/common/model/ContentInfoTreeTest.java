package store.common.model;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import static java.util.Arrays.asList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.fest.assertions.api.Assertions.assertThat;
import org.fest.assertions.api.IterableAssert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.common.hash.Hash;
import store.common.model.ContentInfo.ContentInfoBuilder;
import store.common.model.ContentInfoTree.ContentInfoTreeBuilder;
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
        //
        //     D5
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
                      ImmutableMap.of("bool", Value.of(false),
                                      "msg", Value.of("hello world")),
                      "a1");

        a3 = revision("a3",
                      ImmutableMap.of("bool", Value.of(true),
                                      "msg", Value.of("hello world")),
                      "a2");

        b2 = revision("b2",
                      ImmutableMap.of("answer", Value.of(42),
                                      "msg", Value.of("hello")),
                      "a1");

        c3 = revision("c3",
                      ImmutableMap.of("answer", Value.of(42),
                                      "msg", Value.of("hello you")),
                      "b2");

        a4 = revision("a4",
                      ImmutableMap.of("answer", Value.of(42),
                                      "bool", Value.of(true),
                                      "msg", Value.of("hello world")),
                      "a3", "b2");

        d4 = deleted("d4", "c3");
        d5 = deleted("d5", "a4", "c3");

    }

    private static ContentInfo revision(String rev, String... parents) {
        return builder(parents)
                .build(new Hash(rev));
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
                .withContent(new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"))
                .withLength(1024);

        for (String parent : parents) {
            builder.withParent(new Hash(parent));
        }
        return builder;
    }

    private static Object[] treeData(ContentInfo... revisions) {
        return new Object[]{tree(revisions)};
    }

    private static ContentInfoTree tree(ContentInfo... infos) {
        return new ContentInfoTreeBuilder()
                .addAll(asList(infos))
                .build();
    }

    private static Hash[] revisions(ContentInfo... infos) {
        Hash[] revisions = new Hash[infos.length];
        for (int i = 0; i < infos.length; i++) {
            revisions[i] = infos[i].getRevision();
        }
        return revisions;
    }

    /**
     * Test.
     */
    @Test
    public void getTest() {
        ContentInfoTree tree = tree(a0, a1, a2, a3, b2, a4);
        Hash rev = a3.getRevision();

        assertThat(tree.get(rev)).isEqualTo(a3);
    }

    /**
     * Test.
     */
    @Test
    public void getBulkTest() {
        ContentInfoTree tree = tree(a0, a1, a2, a3, b2, a4);
        Collection<Hash> revs = Arrays.asList(b2.getRevision(), a4.getRevision());

        assertThat(tree.get(revs)).containsExactly(b2, a4);
    }

    /**
     * Test.
     */
    @Test
    public void containsTest() {
        ContentInfoTree tree = tree(a0, a1, a2, a3, b2, a4);

        assertThat(tree.contains(a3.getRevision())).isTrue();
        assertThat(tree.contains(d5.getRevision())).isFalse();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = NoSuchElementException.class)
    public void getUnknownTest() {
        tree(a0, a1, a2, a3, b2, a4).get(d5.getRevision());
    }

    /**
     * Test.
     */
    @Test
    public void hashcodeTest() {
        ContentInfoTree tree = tree(a0, a1, a2, a3, b2, a4);

        assertThat(tree.hashCode()).isEqualTo(tree(a0, a1, a2, a3, b2, a4).hashCode());
        assertThat(tree.hashCode()).isNotEqualTo(tree(a0, a1, a2, a3).hashCode());
    }

    /**
     * Test.
     */
    @Test
    @SuppressWarnings({"IncompatibleEquals", "ObjectEqualsNull"})
    public void equalsTest() {
        ContentInfoTree tree = tree(a0, a1, a2, a3, b2, a4);

        assertThat(tree.equals(tree(a0, a1, a2, a3, b2, a4))).isTrue();
        assertThat(tree.equals(tree(a0, a1, a2, a3))).isFalse();
        assertThat(tree.equals(null)).isFalse();
        assertThat(tree.equals("some text")).isFalse();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "listTestDataProvider")
    public Object[][] listTestDataProvider() {
        return new Object[][]{
            treeData(a0),
            treeData(a0, a1),
            treeData(a0, a1, a2, a3),
            treeData(a0, a1, a2, a3, a4, b2),
            treeData(a0, a1, a2, a3, a4, b2, c3, d4, d5),
            treeData(a0, a1, a2, a3, a4, b2, c3, d4, d5, revision("f0"), revision("f1", "f0"))
        };
    }

    /**
     * Test.
     *
     * @param tree Input tree
     */
    @Test(dataProvider = "listTestDataProvider")
    public void listTest(ContentInfoTree tree) {
        List<ContentInfo> list = tree.list();
        for (ContentInfo info : list) {
            for (Hash parent : info.getParents()) {
                assertOrder(list, info, tree.get(parent));
            }
        }
    }

    private static void assertOrder(List<ContentInfo> list, ContentInfo first, ContentInfo second) {
        assertThat(list.indexOf(first))
                .as(format(list) + ": " + first.getRevision() + " < " + second.getRevision())
                .isLessThan(list.indexOf(second));
    }

    private static String format(List<ContentInfo> list) {
        return Lists.transform(list, new Function<ContentInfo, Hash>() {
            @Override
            public Hash apply(ContentInfo info) {
                return info.getRevision();
            }
        }).toString();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getHeadTestDataProvider")
    public Object[][] getHeadTestDataProvider() {
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
    @Test(dataProvider = "getHeadTestDataProvider")
    public void getHeadTest(ContentInfoTree tree, Hash[] expected) {
        assertThat(tree.getHead()).containsExactly(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getTailTestDataProvider")
    public Object[][] getTailTestDataProvider() {
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
    @Test(dataProvider = "getTailTestDataProvider")
    public void getTailTest(ContentInfoTree tree, Hash[] expected) {
        assertThat(tree.getTail()).containsExactly(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getUnknownParentsTestDataProvider")
    public Object[][] getUnknownParentsTestDataProvider() {
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
    @Test(dataProvider = "getUnknownParentsTestDataProvider")
    public void getUnknownParentsTest(ContentInfoTree tree, Hash[] expected) {
        IterableAssert<Hash> assertion = assertThat(tree.getUnknownParents());
        if (expected.length == 0) {
            assertion.isEmpty();
        } else {
            assertion.containsExactly(expected);
        }
    }

    /**
     * Test.
     */
    @Test
    public void addRevisionTest() {
        assertThat(tree(a0, a1, a2).add(a3)).isEqualTo(tree(a0, a1, a2, a3));
        assertThat(tree(a0, a1, a2, a3).add(a3)).isEqualTo(tree(a0, a1, a2, a3));
    }

    /**
     * Test.
     */
    @Test
    public void addTreeTest() {
        assertThat(tree(a0, a1, a2).add(tree(a3, a4, b2))).isEqualTo(tree(a0, a1, a2, a3, a4, b2));
        assertThat(tree(a0, a1, a2, a3, a4).add(tree(a3, a4, b2))).isEqualTo(tree(a0, a1, a2, a3, a4, b2));
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "noMergeTestDataProvider")
    public Object[][] noMergeTestDataProvider() {
        return new Object[][]{
            treeData(a0),
            treeData(a3, b2),
            treeData(a1, a2, a3),
            treeData(a0, a1, a2, a3),
            treeData(a0, a1, a2, a3, a4, b2),
            treeData(a0, a1, a2, a3, b2, c3),
            treeData(a0, a1, a2, a3, a4, b2, c3),
            treeData(a0, a1, a2, a3, a4, b2, c3, d4),
            treeData(a0, a1, a2, a3, a4, b2, c3, d5)
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
        ContentInfo head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(a4.getMetadata());
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
        ContentInfo head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("msg", Value.of("hello")));
    }

    /**
     * Test.
     */
    @Test
    public void deletedMergeTest() {
        ContentInfoTree merge = tree(a0, a1, a2, a3, a4, b2, c3, d4, d5).merge();
        ContentInfo head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.isDeleted()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void noAncestorMergeTest() {
        ContentInfo rev1a = revision("1a", ImmutableMap.of("msg", Value.of("hello")));
        ContentInfo rev1b = revision("1b", ImmutableMap.of("int", Value.of(10)));

        ContentInfoTree merge = tree(rev1a, rev1b).merge();
        ContentInfo head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("int", Value.of(10),
                                                                 "msg", Value.of("hello")));
    }

    /**
     * Test.
     */
    @Test
    public void crissCrossMergeTest() {
        ContentInfo rev0 = revision("00",
                                    ImmutableMap.of("int", Value.of(5),
                                                    "msg", Value.of("good morning")));

        ContentInfo rev1a = revision("1a",
                                     ImmutableMap.of("int", Value.of(5),
                                                     "msg", Value.of("hello")),
                                     "00");

        ContentInfo rev1b = revision("1b",
                                     ImmutableMap.of("int", Value.of(10),
                                                     "msg", Value.of("good morning")),
                                     "00");

        ContentInfo rev2a = revision("2a",
                                     ImmutableMap.of("int", Value.of(10),
                                                     "msg", Value.of("hello world")),
                                     "1a", "1b");

        ContentInfo rev2b = revision("2b",
                                     ImmutableMap.of("int", Value.of(20),
                                                     "msg", Value.of("hello")),
                                     "1a", "1b");

        ContentInfoTree merge = tree(rev0, rev1a, rev1b, rev2a, rev2b).merge();
        ContentInfo head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("int", Value.of(20),
                                                                 "msg", Value.of("hello world")));
    }

    /**
     * Test.
     */
    @Test
    public void tripleCrissCrossMergeTest() {
        ContentInfo rev0 = revision("00",
                                    ImmutableMap.of("int", Value.of(5),
                                                    "msg", Value.of("good morning"),
                                                    "out", Value.of("good evening")));

        ContentInfo rev1b = revision("1a",
                                     ImmutableMap.of("int", Value.of(10),
                                                     "msg", Value.of("good morning"),
                                                     "out", Value.of("good evening")),
                                     "00");

        ContentInfo rev1a = revision("1b",
                                     ImmutableMap.of("int", Value.of(5),
                                                     "msg", Value.of("hello"),
                                                     "out", Value.of("good evening")),
                                     "00");

        ContentInfo rev1c = revision("1c",
                                     ImmutableMap.of("int", Value.of(5),
                                                     "msg", Value.of("good morning"),
                                                     "out", Value.of("good bye")),
                                     "00");


        ContentInfo rev2a = revision("2a",
                                     ImmutableMap.of("int", Value.of(20),
                                                     "msg", Value.of("hello"),
                                                     "out", Value.of("good bye")),
                                     "1a", "1b", "1c");

        ContentInfo rev2b = revision("2b",
                                     ImmutableMap.of("int", Value.of(10),
                                                     "msg", Value.of("hello world"),
                                                     "out", Value.of("good bye")),
                                     "1a", "1b");

        ContentInfo rev2c = revision("2c",
                                     ImmutableMap.of("int", Value.of(10),
                                                     "msg", Value.of("hello"),
                                                     "out", Value.of("bye bye")),
                                     "1a", "1b", "1c");

        ContentInfoTree merge = tree(rev0, rev1a, rev2a, rev1c, rev1b, rev2b, rev2c).merge();
        ContentInfo head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("int", Value.of(20),
                                                                 "msg", Value.of("hello world"),
                                                                 "out", Value.of("bye bye")));
    }
}
