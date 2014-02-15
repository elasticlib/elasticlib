package store.common;

import java.util.Collection;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Represents a content info revision tree.
 */
public class ContentInfoTree {

    private final SortedSet<Hash> head;
    private final SortedSet<Hash> tail;
    private final SortedSet<Hash> unknownParents;
    private final Map<Hash, ContentInfo> nodes;

    private ContentInfoTree(Map<Hash, ContentInfo> nodes) {
        this.nodes = unmodifiableMap(nodes);
        head = unmodifiableSortedSet(head(nodes));
        tail = unmodifiableSortedSet(tail(nodes));
        unknownParents = unmodifiableSortedSet(unknownParents(nodes));
    }

    private static SortedSet<Hash> head(Map<Hash, ContentInfo> nodes) {
        Set<Hash> nonRoots = new HashSet<>(nodes.size());
        for (ContentInfo info : nodes.values()) {
            nonRoots.addAll(info.getParents());
        }
        SortedSet<Hash> head = new TreeSet<>();
        for (ContentInfo info : nodes.values()) {
            if (!nonRoots.contains(info.getRev())) {
                head.add(info.getRev());
            }
        }
        return head;
    }

    private static SortedSet<Hash> tail(Map<Hash, ContentInfo> nodes) {
        SortedSet<Hash> tail = new TreeSet<>();
        for (ContentInfo info : nodes.values()) {
            if (info.getParents().isEmpty()) {
                tail.add(info.getRev());
            }
        }
        return tail;
    }

    private static SortedSet<Hash> unknownParents(Map<Hash, ContentInfo> nodes) {
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
     * Provides head revisions hashes of this tree, those which are not parent of any other ones.
     *
     * @return A sorted set of revision hashes.
     */
    public SortedSet<Hash> getHead() {
        return head;
    }

    /**
     * Provides tail revisions hashes of this tree, those which do not have any parent.
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
     * @param rev A revision hash.
     * @return Associated revision.
     */
    public ContentInfo get(Hash rev) {
        if (!nodes.containsKey(rev)) {
            throw new NoSuchElementException();
        }
        return nodes.get(rev);
    }

    /**
     * @param rev A revision hash.
     * @return true if this tree contains the revision associated to supplied hash.
     */
    public boolean contains(Hash rev) {
        return nodes.containsKey(rev);
    }

    /**
     * Provides all revisions of this tree sorted by topological order (starting from head).
     *
     * @return A collection of revisions.
     */
    public Collection<ContentInfo> values() {
        Collection<ContentInfo> values = new LinkedHashSet<>();
        SortedSet<Hash> current = new TreeSet<>();
        SortedSet<Hash> next = new TreeSet<>();
        current.addAll(head);
        while (!current.isEmpty()) {
            for (Hash rev : current) {
                ContentInfo info = nodes.get(rev);
                values.add(info);
                for (Hash parentRev : info.getParents()) {
                    next.add(parentRev);
                }
            }
            current.clear();
            current.addAll(next);
        }
        return values;
    }

    /**
     * Adds a new node to this tree.
     *
     * @param info revision to add.
     * @return The new resulting tree.
     */
    public ContentInfoTree add(ContentInfo info) {
        InfoTreeBuilder builder = new InfoTreeBuilder();
        builder.addAll(nodes.values());
        builder.add(info);
        return builder.build();
    }

    /**
     * Adds another revision tree to this one.
     *
     * @param tree A revision tree.
     * @return The new resulting tree.
     */
    public ContentInfoTree add(ContentInfoTree tree) {
        InfoTreeBuilder builder = new InfoTreeBuilder();
        builder.addAll(nodes.values());
        builder.addAll(tree.nodes.values());
        return builder.build();
    }

    /**
     * Creates an automatic merge revision. If head of this tree is already a singleton or if head revisions have
     * conflicting differences, simply returns this tree.
     *
     * @return The tree resulting from merge, possibly this one.
     */
    public ContentInfoTree merge() {
        // TODO this is a stub
        return this;
    }

    /**
     * Builder.
     */
    public static class InfoTreeBuilder {

        private final Map<Hash, ContentInfo> nodes = new HashMap<>();

        /**
         * Add a revision.
         *
         * @param info Revision to add.
         * @return this
         */
        public InfoTreeBuilder add(ContentInfo info) {
            nodes.put(info.getRev(), info);
            return this;
        }

        /**
         * Add some revisions.
         *
         * @param infos Revisions to add.
         * @return this
         */
        public InfoTreeBuilder addAll(Collection<ContentInfo> infos) {
            for (ContentInfo info : infos) {
                nodes.put(info.getRev(), info);
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
