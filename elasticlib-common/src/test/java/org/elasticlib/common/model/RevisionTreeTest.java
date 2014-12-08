package org.elasticlib.common.model;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import static java.util.Arrays.asList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import org.elasticlib.common.model.RevisionTree.RevisionTreeBuilder;
import org.elasticlib.common.value.Value;
import static org.fest.assertions.api.Assertions.assertThat;
import org.fest.assertions.api.IterableAssert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class RevisionTreeTest {

    private final Revision a0;
    private final Revision a1;
    private final Revision a2;
    private final Revision a3;
    private final Revision a4;
    private final Revision b2;
    private final Revision c3;
    private final Revision d4;
    private final Revision d5;

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

    private static Revision revision(String rev, String... parents) {
        return builder(parents)
                .build(new Hash(rev));
    }

    private static Revision revision(String rev, Map<String, Value> metadata, String... parents) {
        return builder(parents)
                .withMetadata(metadata)
                .build(new Hash(rev));
    }

    private static Revision deleted(String rev, String... parents) {
        return builder(parents)
                .withDeleted(true)
                .build(new Hash(rev));
    }

    private static RevisionBuilder builder(String... parents) {
        RevisionBuilder builder = new RevisionBuilder()
                .withContent(new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"))
                .withLength(1024);

        for (String parent : parents) {
            builder.withParent(new Hash(parent));
        }
        return builder;
    }

    private static Object[] treeData(Revision... revisions) {
        return new Object[]{tree(revisions)};
    }

    private static RevisionTree tree(Revision... infos) {
        return new RevisionTreeBuilder()
                .addAll(asList(infos))
                .build();
    }

    private static Hash[] revisions(Revision... infos) {
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
        RevisionTree tree = tree(a0, a1, a2, a3, b2, a4);
        Hash rev = a3.getRevision();

        assertThat(tree.get(rev)).isEqualTo(a3);
    }

    /**
     * Test.
     */
    @Test
    public void getBulkTest() {
        RevisionTree tree = tree(a0, a1, a2, a3, b2, a4);
        Collection<Hash> revs = Arrays.asList(b2.getRevision(), a4.getRevision());

        assertThat(tree.get(revs)).containsExactly(b2, a4);
    }

    /**
     * Test.
     */
    @Test
    public void containsTest() {
        RevisionTree tree = tree(a0, a1, a2, a3, b2, a4);

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
        RevisionTree tree = tree(a0, a1, a2, a3, b2, a4);

        assertThat(tree.hashCode()).isEqualTo(tree(a0, a1, a2, a3, b2, a4).hashCode());
        assertThat(tree.hashCode()).isNotEqualTo(tree(a0, a1, a2, a3).hashCode());
    }

    /**
     * Test.
     */
    @Test
    @SuppressWarnings({"IncompatibleEquals", "ObjectEqualsNull"})
    public void equalsTest() {
        RevisionTree tree = tree(a0, a1, a2, a3, b2, a4);

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
    public void listTest(RevisionTree tree) {
        List<Revision> list = tree.list();
        list.forEach(rev -> {
            rev.getParents()
                    .stream()
                    .map(tree::get)
                    .forEach(parent -> assertOrder(list, rev, parent));
        });
    }

    private static void assertOrder(List<Revision> list, Revision first, Revision second) {
        assertThat(list.indexOf(first))
                .as(format(list) + ": " + first.getRevision() + " < " + second.getRevision())
                .isLessThan(list.indexOf(second));
    }

    private static String format(List<Revision> list) {
        return list.stream()
                .map(info -> info.getRevision())
                .collect(toList())
                .toString();
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
    public void getHeadTest(RevisionTree tree, Hash[] expected) {
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
    public void getTailTest(RevisionTree tree, Hash[] expected) {
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
    public void getUnknownParentsTest(RevisionTree tree, Hash[] expected) {
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
    public void noMergeTest(RevisionTree tree) {
        assertThat(tree.merge()).isEqualTo(tree);
    }

    /**
     * Test.
     */
    @Test
    public void simpleThreeWayMergeTest() {
        RevisionTree merge = tree(a0, a1, a2, a3, b2).merge();
        Revision head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(a4.getMetadata());
    }

    /**
     * Test.
     */
    @Test
    public void sameMetadataMergeTest() {
        Revision a1Bis = revision("b1",
                                  ImmutableMap.of("msg", Value.of("hello")),
                                  "a0");

        RevisionTree merge = tree(a0, a1, a1Bis).merge();
        Revision head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("msg", Value.of("hello")));
    }

    /**
     * Test.
     */
    @Test
    public void deletedMergeTest() {
        RevisionTree merge = tree(a0, a1, a2, a3, a4, b2, c3, d4, d5).merge();
        Revision head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.isDeleted()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void noAncestorMergeTest() {
        Revision rev1a = revision("1a", ImmutableMap.of("msg", Value.of("hello")));
        Revision rev1b = revision("1b", ImmutableMap.of("int", Value.of(10)));

        RevisionTree merge = tree(rev1a, rev1b).merge();
        Revision head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("int", Value.of(10),
                                                                 "msg", Value.of("hello")));
    }

    /**
     * Test.
     */
    @Test
    public void crissCrossMergeTest() {
        Revision rev0 = revision("00",
                                 ImmutableMap.of("int", Value.of(5),
                                                 "msg", Value.of("good morning")));

        Revision rev1a = revision("1a",
                                  ImmutableMap.of("int", Value.of(5),
                                                  "msg", Value.of("hello")),
                                  "00");

        Revision rev1b = revision("1b",
                                  ImmutableMap.of("int", Value.of(10),
                                                  "msg", Value.of("good morning")),
                                  "00");

        Revision rev2a = revision("2a",
                                  ImmutableMap.of("int", Value.of(10),
                                                  "msg", Value.of("hello world")),
                                  "1a", "1b");

        Revision rev2b = revision("2b",
                                  ImmutableMap.of("int", Value.of(20),
                                                  "msg", Value.of("hello")),
                                  "1a", "1b");

        RevisionTree merge = tree(rev0, rev1a, rev1b, rev2a, rev2b).merge();
        Revision head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("int", Value.of(20),
                                                                 "msg", Value.of("hello world")));
    }

    /**
     * Test.
     */
    @Test
    public void tripleCrissCrossMergeTest() {
        Revision rev0 = revision("00",
                                 ImmutableMap.of("int", Value.of(5),
                                                 "msg", Value.of("good morning"),
                                                 "out", Value.of("good evening")));

        Revision rev1b = revision("1a",
                                  ImmutableMap.of("int", Value.of(10),
                                                  "msg", Value.of("good morning"),
                                                  "out", Value.of("good evening")),
                                  "00");

        Revision rev1a = revision("1b",
                                  ImmutableMap.of("int", Value.of(5),
                                                  "msg", Value.of("hello"),
                                                  "out", Value.of("good evening")),
                                  "00");

        Revision rev1c = revision("1c",
                                  ImmutableMap.of("int", Value.of(5),
                                                  "msg", Value.of("good morning"),
                                                  "out", Value.of("good bye")),
                                  "00");

        Revision rev2a = revision("2a",
                                  ImmutableMap.of("int", Value.of(20),
                                                  "msg", Value.of("hello"),
                                                  "out", Value.of("good bye")),
                                  "1a", "1b", "1c");

        Revision rev2b = revision("2b",
                                  ImmutableMap.of("int", Value.of(10),
                                                  "msg", Value.of("hello world"),
                                                  "out", Value.of("good bye")),
                                  "1a", "1b");

        Revision rev2c = revision("2c",
                                  ImmutableMap.of("int", Value.of(10),
                                                  "msg", Value.of("hello"),
                                                  "out", Value.of("bye bye")),
                                  "1a", "1b", "1c");

        RevisionTree merge = tree(rev0, rev1a, rev2a, rev1c, rev1b, rev2b, rev2c).merge();
        Revision head = merge.get(merge.getHead().first());

        assertThat(merge.getHead()).hasSize(1);
        assertThat(head.getMetadata()).isEqualTo(ImmutableMap.of("int", Value.of(20),
                                                                 "msg", Value.of("hello world"),
                                                                 "out", Value.of("bye bye")));
    }
}
