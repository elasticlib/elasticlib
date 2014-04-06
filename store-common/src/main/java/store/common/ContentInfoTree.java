package store.common;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import store.common.ContentInfo.ContentInfoBuilder;
import static store.common.Diff.diff;
import static store.common.Diff.mergeDiff;
import store.common.hash.Hash;
import store.common.value.Value;

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
        List<ContentInfo> list = new ArrayList<>();
        List<ContentInfo> toAdd = new ArrayList<>();
        Set<Hash> sorted = new HashSet<>();
        Set<Hash> current = new HashSet<>();
        Set<Hash> next = new HashSet<>();
        current.addAll(head);
        while (!current.isEmpty()) {
            for (Hash rev : difference(current, sorted)) {
                if (contains(rev)) {
                    ContentInfo info = get(rev);
                    toAdd.add(info);
                    for (Hash parentRev : info.getParents()) {
                        next.add(parentRev);
                    }
                }
            }
            Collections.sort(toAdd, new TopologicalComparator());
            moveAll(toAdd, list);
            moveAll(current, sorted);
            moveAll(next, current);
        }
        return list;
    }

    private <T> void moveAll(Collection<T> src, Collection<T> dest) {
        dest.addAll(src);
        src.clear();
    }

    private final class TopologicalComparator implements Comparator<  ContentInfo> {

        @Override
        public int compare(ContentInfo info1, ContentInfo info2) {
            if (ancestors(info1).contains(info2)) {
                return -1;
            }
            if (ancestors(info2).contains(info1)) {
                return 1;
            }
            return 0;
        }
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
        Optional<Diff> diff = mergeDiff(diff(base, left.getMetadata()),
                                        diff(base, right.getMetadata()));
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
}
