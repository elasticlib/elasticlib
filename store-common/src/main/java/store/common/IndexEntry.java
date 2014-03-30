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
import store.common.value.Value;

/**
 * Represents an entry about a content in an index.
 */
public final class IndexEntry implements Mappable {

    private static final String HASH = "hash";
    private static final String HEAD = "head";
    private final Hash hash;
    private final SortedSet<Hash> head;

    /**
     * Constructor.
     *
     * @param hash Content hash.
     * @param head Hashes of the content head revisions.
     */
    public IndexEntry(Hash hash, Set<Hash> head) {
        this.hash = hash;
        this.head = unmodifiableSortedSet(new TreeSet<>(head));
    }

    /**
     * @return The content hash.
     */
    public Hash getHash() {
        return hash;
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
                .put(HASH, hash)
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
        return new IndexEntry(new Hash(map.get(HASH).asByteArray()),
                              fromList(map.get(HEAD).asList()));
    }

    @Override
    public int hashCode() {
        return hash(hash, head);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexEntry)) {
            return false;
        }
        IndexEntry other = (IndexEntry) obj;
        return new EqualsBuilder()
                .append(hash, other.hash)
                .append(head, other.head)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(HASH, hash)
                .add(HEAD, head)
                .toString();
    }
}
