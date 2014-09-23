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
import store.common.model.ContentInfo.ContentInfoBuilder;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Represents a content info revision tree.
 */
public class ContentInfoTree implements Mappable {

    private static final String CONTENT = "content";
    private static final String LENGTH = "length";
    private static final String REVISIONS = "revisions";
    private final SortedSet<Hash> head;
    private final SortedSet<Hash> tail;
    private final SortedSet<Hash> unknownParents;
    private final Map<Hash, ContentInfo> nodes;

    private ContentInfoTree(Map<Hash, ContentInfo> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.nodes = unmodifiableMap(nodes);
        head = unmodifiableSortedSet(buildHead(nodes));
        tail = unmodifiableSortedSet(buildTail(nodes));
        unknownParents = unmodifiableSortedSet(buildUnknownParents(nodes));
    }

    private static SortedSet<Hash> buildHead(Map<Hash, ContentInfo> nodes) {
        Set<Hash> nonRoots = new HashSet<>(nodes.size());
        for (ContentInfo info : nodes.values()) {
            nonRoots.addAll(info.getParents());
        }
        SortedSet<Hash> head = new TreeSet<>();
        for (ContentInfo info : nodes.values()) {
            if (!nonRoots.contains(info.getRevision())) {
                head.add(info.getRevision());
            }
        }
        return head;
    }

    private static SortedSet<Hash> buildTail(Map<Hash, ContentInfo> nodes) {
        SortedSet<Hash> tail = new TreeSet<>();
        for (ContentInfo info : nodes.values()) {
            if (intersection(info.getParents(), nodes.keySet()).isEmpty()) {
                tail.add(info.getRevision());
            }
        }
        return tail;
    }

    private static SortedSet<Hash> buildUnknownParents(Map<Hash, ContentInfo> nodes) {
        SortedSet<Hash> unknownParents = new TreeSet<>();
        for (ContentInfo info : nodes.values()) {
            for (Hash parent : info.getParents()) {
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
    public ContentInfo get(Hash revision) {
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
    public List<ContentInfo> get(Collection<Hash> revs) {
        List<ContentInfo> revisions = new ArrayList<>(revs.size());
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
    public List<ContentInfo> list() {
        return new TopologicalSort(nodes, head).sort();
    }

    /**
     * Adds a new node to this tree.
     *
     * @param info revision to add.
     * @return The new resulting tree.
     */
    public ContentInfoTree add(ContentInfo info) {
        return new ContentInfoTreeBuilder()
                .addAll(nodes.values())
                .add(info)
                .build();
    }

    /**
     * Adds another revision tree to this one.
     *
     * @param tree A revision tree.
     * @return The new resulting tree.
     */
    public ContentInfoTree add(ContentInfoTree tree) {
        return new ContentInfoTreeBuilder()
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
    public ContentInfoTree merge() {
        if (head.size() <= 1) {
            return this;
        }
        Iterator<Hash> headIt = head.iterator();
        ContentInfo mergeHead = get(headIt.next());
        ContentInfoTree workTree = this;
        while (headIt.hasNext()) {
            Optional<ContentInfo> merge = workTree.merge(mergeHead, get(headIt.next()));
            if (!merge.isPresent()) {
                return this;
            }
            mergeHead = merge.get();
            if (headIt.hasNext()) {
                workTree = workTree.add(mergeHead);
            }
        }
        return add(new ContentInfoBuilder()
                .withContent(mergeHead.getContent())
                .withLength(mergeHead.getLength())
                .withParents(head)
                .withDeleted(mergeHead.isDeleted())
                .withMetadata(mergeHead.getMetadata())
                .computeRevisionAndBuild());
    }

    private Optional<ContentInfo> merge(ContentInfo left, ContentInfo right) {
        if (left.isDeleted() != right.isDeleted()) {
            // No automatic merge in this particular case, it is a conflict.
            return Optional.absent();
        }
        if (left.getMetadata().equals(right.getMetadata())) {
            return threeWayMerge(left, right, left.getMetadata());
        }
        Set<ContentInfo> ancestors = latestCommonAncestors(left, right);
        if (ancestors.isEmpty()) {
            return threeWayMerge(left, right, Collections.<String, Value>emptyMap());
        }
        if (ancestors.size() == 1) {
            return threeWayMerge(left, right, ancestors.iterator().next().getMetadata());
        }
        return recursiveThreeWayMerge(left, right, ancestors);
    }

    private Optional<ContentInfo> recursiveThreeWayMerge(ContentInfo left, ContentInfo right, Set<ContentInfo> ancestors) {
        Iterator<ContentInfo> ancestorsIt = ancestors.iterator();
        ContentInfo virtualAncestor = ancestorsIt.next();
        ContentInfoTree workTree = this;
        while (ancestorsIt.hasNext()) {
            Optional<ContentInfo> merge = workTree.merge(virtualAncestor, ancestorsIt.next());
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

    private Optional<ContentInfo> threeWayMerge(ContentInfo left, ContentInfo right, Map<String, Value> base) {
        Optional<Diff> diff = Diff.merge(Diff.of(base, left.getMetadata()),
                                         Diff.of(base, right.getMetadata()));
        if (!diff.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(new ContentInfoBuilder()
                .withContent(left.getContent())
                .withLength(left.getLength())
                .withParent(left.getRevision())
                .withParent(right.getRevision())
                .withDeleted(left.isDeleted() && right.isDeleted())
                .withMetadata(diff.get().apply(base))
                .computeRevisionAndBuild());
    }

    private Set<ContentInfo> latestCommonAncestors(ContentInfo left, ContentInfo right) {
        Set<ContentInfo> commonAncestors = intersection(ancestors(left), ancestors(right));

        SetMultimap<ContentInfo, ContentInfo> dependencies = HashMultimap.create();
        for (ContentInfo info : commonAncestors) {
            dependencies.putAll(info, intersection(commonAncestors, ancestors(info)));
        }
        return difference(commonAncestors, new HashSet<>(dependencies.values()));
    }

    private Set<ContentInfo> ancestors(ContentInfo info) {
        return ancestors(info, new HashSet<ContentInfo>());
    }

    private Set<ContentInfo> ancestors(ContentInfo info, Set<ContentInfo> seed) {
        for (Hash rev : info.getParents()) {
            if (contains(rev)) {
                ContentInfo parent = get(rev);
                seed.add(parent);
                ancestors(parent, seed);
            }
        }
        return seed;
    }

    @Override
    public Map<String, Value> toMap() {
        List<Value> revisions = new ArrayList<>(nodes.size());
        for (ContentInfo info : list()) {
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
    public static ContentInfoTree fromMap(Map<String, Value> map) {
        ContentInfoTreeBuilder builder = new ContentInfoTreeBuilder();
        for (Value revision : map.get(REVISIONS).asList()) {
            Map<String, Value> infoMap = new HashMap<>();
            infoMap.put(CONTENT, map.get(CONTENT));
            infoMap.put(LENGTH, map.get(LENGTH));
            infoMap.putAll(revision.asMap());
            builder.add(ContentInfo.fromMap(infoMap));
        }
        return builder.build();
    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ContentInfoTree)) {
            return false;
        }
        ContentInfoTree other = (ContentInfoTree) obj;
        return nodes.equals(other.nodes);
    }

    @Override
    public String toString() {
        return nodes.values().toString();
    }

    /**
     * Builder.
     */
    public static class ContentInfoTreeBuilder {

        private final Map<Hash, ContentInfo> nodes = new HashMap<>();

        /**
         * Add a revision.
         *
         * @param info Revision to add.
         * @return this
         */
        public ContentInfoTreeBuilder add(ContentInfo info) {
            nodes.put(info.getRevision(), info);
            return this;
        }

        /**
         * Add some revisions.
         *
         * @param infos Revisions to add.
         * @return this
         */
        public ContentInfoTreeBuilder addAll(Collection<ContentInfo> infos) {
            for (ContentInfo info : infos) {
                nodes.put(info.getRevision(), info);
            }
            return this;
        }

        /**
         * Build tree.
         *
         * @return A new ContentInfoTree instance.
         */
        public ContentInfoTree build() {
            return new ContentInfoTree(nodes);
        }
    }

    /**
     * A simple topological sorting algorithm based on a depth-first search.
     */
    private static class TopologicalSort {

        private final Map<Hash, ContentInfo> unsorted;
        private final Collection<Hash> head;
        private final List<ContentInfo> sorted = new ArrayList<>();

        /**
         * Constructor.
         *
         * @param nodes Nodes to sort, mapped by their revisions hashes.
         * @param head head revisions hashes.
         */
        public TopologicalSort(Map<Hash, ContentInfo> nodes, Collection<Hash> head) {
            unsorted = new HashMap<>(nodes);
            this.head = head;
        }

        /**
         * Sorts nodes in topological order. Any node in the returned list is always before all its parents.
         *
         * @return A topologically ordered list.
         */
        public List<ContentInfo> sort() {
            for (Hash seed : head) {
                visit(unsorted.get(seed));
            }
            // Nodes are actually sorted in reverse order at this point.
            Collections.reverse(sorted);
            return sorted;
        }

        private void visit(ContentInfo info) {
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
