package store.common;

import java.util.Collection;
import static java.util.Collections.unmodifiableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Represents a revision tree of all info about a content.
 */
public class ContentInfoTree {

    private final SortedSet<ContentInfo> roots;
    private final Map<Hash, ContentInfo> nodes;

    private ContentInfoTree(Map<Hash, ContentInfo> nodes) {
        this.nodes = unmodifiableMap(nodes);
        Set<Hash> nonRoots = new HashSet<>(nodes.size());
        for (ContentInfo info : nodes.values()) {
            nonRoots.addAll(info.getParents());
        }
        SortedSet<ContentInfo> tmp = newSortedSet();
        for (ContentInfo info : nodes.values()) {
            if (!nonRoots.contains(info.getRev())) {
                tmp.add(info);
            }
        }
        roots = new UnmodifiableSortedSet<>(tmp);
    }

    public Set<ContentInfo> getRoots() {
        return roots;
    }

    public ContentInfo getHead() {
        return roots.first();
    }

    public boolean hasConflict() {
        return roots.size() > 1;
    }

    public ContentInfo get(Hash rev) {
        return nodes.get(rev);
    }

    /**
     * Provide all revisions of this tree sorted by topological order.
     *
     * @return A collection of revisions.
     */
    public Collection<ContentInfo> values() {
        Collection<ContentInfo> values = new LinkedHashSet<>();
        SortedSet<ContentInfo> current = newSortedSet();
        SortedSet<ContentInfo> next = newSortedSet();
        current.addAll(roots);
        while (!current.isEmpty()) {
            values.addAll(current);
            for (ContentInfo info : current) {
                for (Hash rev : info.getParents()) {
                    next.add(nodes.get(rev));
                }
            }
            current.clear();
            current.addAll(next);
        }
        return values;
    }

    /**
     * Add a new Head to this tree. Fails if supplied info does not have this tree roots as parents.
     *
     * @param info revision to add.
     * @return The resulting tree.
     */
    public ContentInfoTree add(ContentInfo info) {
        SortedSet<Hash> expectedParents = new TreeSet<>();
        for (ContentInfo root : roots) {
            expectedParents.add(root.getRev());
        }
        if (!expectedParents.equals(info.getParents())) {
            throw new IllegalArgumentException(); // TODO Throw a more appropriate exception !
        }
        InfoTreeBuilder builder = new InfoTreeBuilder();
        builder.addAll(nodes.values());
        builder.add(info);
        return builder.build();
    }

    /**
     * Merge two revision trees.
     *
     * @param t1 First tree.
     * @param t2 Second tree.
     * @return A new Tree.
     */
    public static ContentInfoTree merge(ContentInfoTree t1, ContentInfoTree t2) {
        InfoTreeBuilder builder = new InfoTreeBuilder();
        builder.addAll(t1.nodes.values());
        builder.addAll(t2.nodes.values());

        // TODO if possible, resolve any conflict here by creating a merge-revision.
        return builder.build();
    }

    /**
     * Builder.
     */
    public static class InfoTreeBuilder {

        private final Map<Hash, ContentInfo> nodes = new HashMap<>();

        public InfoTreeBuilder add(ContentInfo info) {
            nodes.put(info.getRev(), info);
            return this;
        }

        public InfoTreeBuilder addAll(Collection<ContentInfo> infos) {
            for (ContentInfo info : infos) {
                nodes.put(info.getRev(), info);
            }
            return this;
        }

        public ContentInfoTree build() {
            return new ContentInfoTree(nodes);
        }
    }

    private static SortedSet<ContentInfo> newSortedSet() {
        return new TreeSet<>(RevisionComparator.INSTANCE);
    }

    private static class RevisionComparator implements Comparator<ContentInfo> {

        public static final RevisionComparator INSTANCE = new RevisionComparator();

        @Override
        public int compare(ContentInfo o1, ContentInfo o2) {
            return o1.getRev().compareTo(o2.getRev());
        }
    }
}
