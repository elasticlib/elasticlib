package store.common;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Map;
import static java.util.Objects.hash;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import static store.common.MappableUtil.fromList;
import static store.common.MappableUtil.toList;
import store.common.hash.Hash;
import store.common.value.Value;

/**
 * Represents an entry about a content in an index.
 */
public final class IndexEntry implements Mappable {

    private static final String CONTENT = "content";
    private static final String HEAD = "head";
    private final Hash content;
    private final SortedSet<Hash> head;

    /**
     * Constructor.
     *
     * @param content Content hash.
     * @param head Hashes of the content head revisions.
     */
    public IndexEntry(Hash content, Set<Hash> head) {
        this.content = content;
        this.head = unmodifiableSortedSet(new TreeSet<>(head));
    }

    /**
     * @return The content hash.
     */
    public Hash getHash() {
        return content;
    }

    /**
     * @return Hashes of the content head revisions.
     */
    public SortedSet<Hash> getHead() {
        return head;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(CONTENT, content)
                .put(HEAD, toList(head))
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static IndexEntry fromMap(Map<String, Value> map) {
        return new IndexEntry(map.get(CONTENT).asHash(),
                              fromList(map.get(HEAD).asList()));
    }

    @Override
    public int hashCode() {
        return hash(content, head);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexEntry)) {
            return false;
        }
        IndexEntry other = (IndexEntry) obj;
        return new EqualsBuilder()
                .append(content, other.content)
                .append(head, other.head)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(CONTENT, content)
                .add(HEAD, head)
                .toString();
    }
}
