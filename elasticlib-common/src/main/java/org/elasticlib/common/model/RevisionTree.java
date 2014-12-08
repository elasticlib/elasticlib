package org.elasticlib.common.model;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;

/**
 * Represents metadata revision tree of a given content.
 */
public class RevisionTree implements Mappable {

    private static final String CONTENT = "content";
    private static final String LENGTH = "length";
    private static final String REVISIONS = "revisions";
    private final SortedSet<Hash> head;
    private final SortedSet<Hash> tail;
    private final SortedSet<Hash> unknownParents;
    private final Map<Hash, Revision> nodes;

    private RevisionTree(Map<Hash, Revision> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.nodes = unmodifiableMap(nodes);
        head = unmodifiableSortedSet(buildHead(nodes));
        tail = unmodifiableSortedSet(buildTail(nodes));
        unknownParents = unmodifiableSortedSet(buildUnknownParents(nodes));
    }

    private static SortedSet<Hash> buildHead(Map<Hash, Revision> nodes) {
        Set<Hash> nonRoots = nodes.values()
                .stream()
                .flatMap(rev -> rev.getParents().stream())
                .collect(toSet());

        return nodes.values()
                .stream()
                .filter(info -> !nonRoots.contains(info.getRevision()))
                .map(info -> info.getRevision())
                .collect(toCollection(TreeSet::new));
    }

    private static SortedSet<Hash> buildTail(Map<Hash, Revision> nodes) {
        return nodes.values()
                .stream()
                .filter(rev -> intersection(rev.getParents(), nodes.keySet()).isEmpty())
                .map(rev -> rev.getRevision())
                .collect(toCollection(TreeSet::new));
    }

    private static SortedSet<Hash> buildUnknownParents(Map<Hash, Revision> nodes) {
        return nodes.values()
                .stream()
                .flatMap(rev -> rev.getParents().stream())
                .filter(parent -> !nodes.containsKey(parent))
                .collect(toCollection(TreeSet::new));
    }

    /**
     * Provides associated content hash.
     *
     * @return A hash.
     */
    public Hash getContent() {
        return nodes.values().iterator().next().getContent();
    }

    /**
     * Provides associated content length.
     *
     * @return A content length in bytes.
     */
    public long getLength() {
        return nodes.values().iterator().next().getLength();
    }

    /**
     * Indicates if content is deleted according to head.
     *
     * @return true if all revisions at head are deleted.
     */
    public boolean isDeleted() {
        return head.stream().allMatch(rev -> nodes.get(rev).isDeleted());
    }

    /**
     * Provides head revisions hashes of this tree, those which are not parent of any other ones.
     *
     * @return A sorted set of revision hashes.
     */
    public SortedSet<Hash> getHead() {
        return head;
    }

    /**
     * Provides tail revisions hashes of this tree, those which do not have any parent. Tail also includes revisions for
     * which all parents are unknown.
     *
     * @return A sorted set of revision hashes.
     */
    public SortedSet<Hash> getTail() {
        return tail;
    }

    /**
     * Provides unknown parents revisions hashes of this tree, those which are referenced as a parent of a revision in
     * this tree, but are themselve absent.
     *
     * @return A sorted set of revision hashes.
     */
    public SortedSet<Hash> getUnknownParents() {
        return unknownParents;
    }

    /**
     * Provides revision associated to supplied hash.
     *
     * @param revision A revision hash.
     * @return Associated revision.
     */
    public Revision get(Hash revision) {
        if (!nodes.containsKey(revision)) {
            throw new NoSuchElementException();
        }
        return nodes.get(revision);
    }

    /**
     * Provides revisions associated to supplied hashes.
     *
     * @param revs A collecton of revision hashes.
     * @return Associated revisions.
     */
    public List<Revision> get(Collection<Hash> revs) {
        return revs.stream()
                .map(rev -> get(rev))
                .collect(toList());
    }

    /**
     * Checks if this tree contains the revision associated to supplied revision hash.
     *
     * @param revision A revision hash.
     * @return true if this tree contains the revision associated to supplied hash.
     */
    public boolean contains(Hash revision) {
        return nodes.containsKey(revision);
    }

    /**
     * Lists all revisions of this tree sorted by topological order (starting from head).
     *
     * @return A collection of revisions.
     */
    public List<Revision> list() {
        return new TopologicalSort(nodes, head).sort();
    }

    /**
     * Adds a new revision to this tree.
     *
     * @param revision revision to add.
     * @return The new resulting tree.
     */
    public RevisionTree add(Revision revision) {
        return new RevisionTreeBuilder()
                .addAll(nodes.values())
                .add(revision)
                .build();
    }

    /**
     * Adds another revision tree to this one.
     *
     * @param tree A revision tree.
     * @return The new resulting tree.
     */
    public RevisionTree add(RevisionTree tree) {
        return new RevisionTreeBuilder()
                .addAll(nodes.values())
                .addAll(tree.nodes.values())
                .build();
    }

    /**
     * Creates an automatic merge revision. If head of this tree is already a singleton or if head revisions have
     * conflicting differences, simply returns this tree.
     *
     * @return The tree resulting from merge, possibly this one.
     */
    public RevisionTree merge() {
        if (head.size() <= 1) {
            return this;
        }
        Iterator<Hash> headIt = head.iterator();
        Revision mergeHead = get(headIt.next());
        RevisionTree workTree = this;
        while (headIt.hasNext()) {
            Optional<Revision> merge = workTree.merge(mergeHead, get(headIt.next()));
            if (!merge.isPresent()) {
                return this;
            }
            mergeHead = merge.get();
            if (headIt.hasNext()) {
                workTree = workTree.add(mergeHead);
            }
        }
        return add(new RevisionBuilder()
                .withContent(mergeHead.getContent())
                .withLength(mergeHead.getLength())
                .withParents(head)
                .withDeleted(mergeHead.isDeleted())
                .withMetadata(mergeHead.getMetadata())
                .computeRevisionAndBuild());
    }

    private Optional<Revision> merge(Revision left, Revision right) {
        if (left.isDeleted() != right.isDeleted()) {
            // No automatic merge in this particular case, it is a conflict.
            return Optional.empty();
        }
        if (left.getMetadata().equals(right.getMetadata())) {
            return threeWayMerge(left, right, left.getMetadata());
        }
        Set<Revision> ancestors = latestCommonAncestors(left, right);
        if (ancestors.isEmpty()) {
            return threeWayMerge(left, right, Collections.<String, Value>emptyMap());
        }
        if (ancestors.size() == 1) {
            return threeWayMerge(left, right, ancestors.iterator().next().getMetadata());
        }
        return recursiveThreeWayMerge(left, right, ancestors);
    }

    private Optional<Revision> recursiveThreeWayMerge(Revision left, Revision right, Set<Revision> ancestors) {
        Iterator<Revision> ancestorsIt = ancestors.iterator();
        Revision virtualAncestor = ancestorsIt.next();
        RevisionTree workTree = this;
        while (ancestorsIt.hasNext()) {
            Optional<Revision> merge = workTree.merge(virtualAncestor, ancestorsIt.next());
            if (!merge.isPresent()) {
                return Optional.empty();
            }
            virtualAncestor = merge.get();
            if (ancestorsIt.hasNext()) {
                workTree = workTree.add(virtualAncestor);
            }
        }
        return threeWayMerge(left, right, virtualAncestor.getMetadata());
    }

    private Optional<Revision> threeWayMerge(Revision left, Revision right, Map<String, Value> base) {
        Optional<Diff> diff = Diff.merge(Diff.of(base, left.getMetadata()),
                                         Diff.of(base, right.getMetadata()));
        if (!diff.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new RevisionBuilder()
                .withContent(left.getContent())
                .withLength(left.getLength())
                .withParent(left.getRevision())
                .withParent(right.getRevision())
                .withDeleted(left.isDeleted() && right.isDeleted())
                .withMetadata(diff.get().apply(base))
                .computeRevisionAndBuild());
    }

    private Set<Revision> latestCommonAncestors(Revision left, Revision right) {
        Set<Revision> commonAncestors = intersection(ancestors(left), ancestors(right));
        SetMultimap<Revision, Revision> dependencies = HashMultimap.create();
        commonAncestors.forEach(info -> {
            dependencies.putAll(info, intersection(commonAncestors, ancestors(info)));
        });
        return difference(commonAncestors, new HashSet<>(dependencies.values()));
    }

    private Set<Revision> ancestors(Revision info) {
        return ancestors(info, new HashSet<>());
    }

    private Set<Revision> ancestors(Revision revision, Set<Revision> seed) {
        revision.getParents()
                .stream()
                .filter(rev -> contains(rev))
                .forEach(rev -> {
                    Revision parent = get(rev);
                    seed.add(parent);
                    ancestors(parent, seed);
                });
        return seed;
    }

    @Override
    public Map<String, Value> toMap() {
        List<Value> revisions = list()
                .stream()
                .map(rev -> {
                    Map<String, Value> map = rev.toMap();
                    map.remove(CONTENT);
                    map.remove(LENGTH);
                    return Value.of(map);
                })
                .collect(toList());

        return new MapBuilder()
                .put(CONTENT, getContent())
                .put(LENGTH, getLength())
                .put(REVISIONS, revisions)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static RevisionTree fromMap(Map<String, Value> map) {
        RevisionTreeBuilder builder = new RevisionTreeBuilder();
        map.get(REVISIONS)
                .asList()
                .stream()
                .forEach((revision) -> {
                    Map<String, Value> revMap = new HashMap<>();
                    revMap.put(CONTENT, map.get(CONTENT));
                    revMap.put(LENGTH, map.get(LENGTH));
                    revMap.putAll(revision.asMap());
                    builder.add(Revision.fromMap(revMap));
                });
        return builder.build();
    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RevisionTree)) {
            return false;
        }
        RevisionTree other = (RevisionTree) obj;
        return nodes.equals(other.nodes);
    }

    @Override
    public String toString() {
        return nodes.values().toString();
    }

    /**
     * Builder.
     */
    public static class RevisionTreeBuilder {

        private final Map<Hash, Revision> nodes = new HashMap<>();

        /**
         * Add a revision.
         *
         * @param rev Revision to add.
         * @return this
         */
        public RevisionTreeBuilder add(Revision rev) {
            nodes.put(rev.getRevision(), rev);
            return this;
        }

        /**
         * Add some revisions.
         *
         * @param revisions Revisions to add.
         * @return this
         */
        public RevisionTreeBuilder addAll(Collection<Revision> revisions) {
            revisions.forEach(rev -> nodes.put(rev.getRevision(), rev));
            return this;
        }

        /**
         * Build tree.
         *
         * @return A new RevisionTree instance.
         */
        public RevisionTree build() {
            return new RevisionTree(nodes);
        }
    }

    /**
     * A simple topological sorting algorithm based on a depth-first search.
     */
    private static class TopologicalSort {

        private final Map<Hash, Revision> unsorted;
        private final Collection<Hash> head;
        private final List<Revision> sorted = new ArrayList<>();

        /**
         * Constructor.
         *
         * @param nodes Nodes to sort, mapped by their revisions hashes.
         * @param head head revisions hashes.
         */
        public TopologicalSort(Map<Hash, Revision> nodes, Collection<Hash> head) {
            unsorted = new HashMap<>(nodes);
            this.head = head;
        }

        /**
         * Sorts nodes in topological order. Any node in the returned list is always before all its parents.
         *
         * @return A topologically ordered list.
         */
        public List<Revision> sort() {
            head.forEach(seed -> visit(unsorted.get(seed)));

            // Nodes are actually sorted in reverse order at this point.
            Collections.reverse(sorted);
            return sorted;
        }

        private void visit(Revision info) {
            info.getParents()
                    .stream()
                    .filter(parent -> unsorted.containsKey(parent))
                    .forEach(parent -> visit(unsorted.get(parent)));

            sorted.add(info);
            unsorted.remove(info.getRevision());
        }
    }

    /**
     * Represents a diff from a set of metadata to another one.
     */
    private static final class Diff {

        private final Map<String, Value> diff;

        private Diff(Map<String, Value> diff) {
            this.diff = unmodifiableMap(new TreeMap<>(diff));
        }

        /**
         * Compute diff between two sets of metadata.
         *
         * @param from Source metadata set.
         * @param to Target metadata set.
         * @return A new Diff instance.
         */
        public static Diff of(Map<String, Value> from, Map<String, Value> to) {
            Map<String, Value> diff = new HashMap<>();
            to.entrySet()
                    .stream()
                    .filter(entry -> !Objects.equals(from.get(entry.getKey()), entry.getValue()))
                    .forEach(entry -> diff.put(entry.getKey(), entry.getValue()));

            from.keySet()
                    .stream()
                    .filter(key -> !to.keySet().contains(key))
                    .forEach(key -> diff.put(key, Value.ofNull()));

            return new Diff(diff);
        }

        /**
         * Merge two diffs. Returns a merge diff if there is no conflict, or nothing otherwise.
         *
         * @param left First diff.
         * @param right Second diff.
         * @return An optional new resulting diff.
         */
        public static Optional<Diff> merge(Diff left, Diff right) {
            Map<String, Value> merge = new HashMap<>(left.diff);
            for (Entry<String, Value> entry : right.diff.entrySet()) {
                String key = entry.getKey();
                Value value = entry.getValue();
                if (merge.containsKey(key) && !merge.get(key).equals(value)) {
                    return Optional.empty();
                }
                if (!merge.containsKey(key)) {
                    merge.put(key, value);
                }
            }
            return Optional.of(new Diff(merge));
        }

        /**
         * Apply this diff to a metadata set.
         *
         * @param source A metadata set.
         * @return Resulting metadata.
         */
        public Map<String, Value> apply(Map<String, Value> source) {
            Map<String, Value> result = new TreeMap<>(source);
            diff.entrySet().forEach(entry -> {
                String key = entry.getKey();
                Value value = entry.getValue();
                if (value.type() == ValueType.NULL) {
                    result.remove(key);
                } else {
                    result.put(key, value);
                }
            });
            return result;
        }
    }
}
