package store.common.model;

import com.google.common.base.Optional;
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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import store.common.hash.Hash;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.model.Revision.RevisionBuilder;
import store.common.value.Value;
import store.common.value.ValueType;

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
        Set<Hash> nonRoots = new HashSet<>(nodes.size());
        for (Revision rev : nodes.values()) {
            nonRoots.addAll(rev.getParents());
        }
        SortedSet<Hash> head = new TreeSet<>();
        for (Revision info : nodes.values()) {
            if (!nonRoots.contains(info.getRevision())) {
                head.add(info.getRevision());
            }
        }
        return head;
    }

    private static SortedSet<Hash> buildTail(Map<Hash, Revision> nodes) {
        SortedSet<Hash> tail = new TreeSet<>();
        for (Revision rev : nodes.values()) {
            if (intersection(rev.getParents(), nodes.keySet()).isEmpty()) {
                tail.add(rev.getRevision());
            }
        }
        return tail;
    }

    private static SortedSet<Hash> buildUnknownParents(Map<Hash, Revision> nodes) {
        SortedSet<Hash> unknownParents = new TreeSet<>();
        for (Revision rev : nodes.values()) {
            for (Hash parent : rev.getParents()) {
                if (!nodes.containsKey(parent)) {
                    unknownParents.add(parent);
                }
            }
        }
        return unknownParents;
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
        for (Hash rev : head) {
            if (!nodes.get(rev).isDeleted()) {
                return false;
            }
        }
        return true;
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
        List<Revision> revisions = new ArrayList<>(revs.size());
        for (Hash rev : revs) {
            revisions.add(get(rev));
        }
        return revisions;
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
            return Optional.absent();
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
                return Optional.absent();
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
            return Optional.absent();
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
        for (Revision info : commonAncestors) {
            dependencies.putAll(info, intersection(commonAncestors, ancestors(info)));
        }
        return difference(commonAncestors, new HashSet<>(dependencies.values()));
    }

    private Set<Revision> ancestors(Revision info) {
        return ancestors(info, new HashSet<Revision>());
    }

    private Set<Revision> ancestors(Revision revision, Set<Revision> seed) {
        for (Hash rev : revision.getParents()) {
            if (contains(rev)) {
                Revision parent = get(rev);
                seed.add(parent);
                ancestors(parent, seed);
            }
        }
        return seed;
    }

    @Override
    public Map<String, Value> toMap() {
        List<Value> revisions = new ArrayList<>(nodes.size());
        for (Revision info : list()) {
            Map<String, Value> map = info.toMap();
            map.remove(CONTENT);
            map.remove(LENGTH);
            revisions.add(Value.of(map));
        }
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
        for (Value revision : map.get(REVISIONS).asList()) {
            Map<String, Value> infoMap = new HashMap<>();
            infoMap.put(CONTENT, map.get(CONTENT));
            infoMap.put(LENGTH, map.get(LENGTH));
            infoMap.putAll(revision.asMap());
            builder.add(Revision.fromMap(infoMap));
        }
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
            for (Revision rev : revisions) {
                nodes.put(rev.getRevision(), rev);
            }
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
            for (Hash seed : head) {
                visit(unsorted.get(seed));
            }
            // Nodes are actually sorted in reverse order at this point.
            Collections.reverse(sorted);
            return sorted;
        }

        private void visit(Revision info) {
            for (Hash parent : info.getParents()) {
                if (unsorted.containsKey(parent)) {
                    visit(unsorted.get(parent));
                }
            }
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
            for (Entry<String, Value> entry : to.entrySet()) {
                String key = entry.getKey();
                Value value = entry.getValue();
                if (!Objects.equals(from.get(key), value)) {
                    diff.put(key, value);
                }
            }
            for (String key : from.keySet()) {
                if (!to.keySet().contains(key)) {
                    diff.put(key, Value.ofNull());
                }
            }
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
                    return Optional.absent();
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
            for (Entry<String, Value> entry : diff.entrySet()) {
                String key = entry.getKey();
                Value value = entry.getValue();
                if (value.type() == ValueType.NULL) {
                    result.remove(key);
                } else {
                    result.put(key, value);
                }
            }
            return result;
        }
    }
}
