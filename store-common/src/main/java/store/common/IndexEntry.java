package store.common;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import static java.util.Objects.hash;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Represents an entry about a content in an index.
 */
public final class IndexEntry {

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
                .add("hash", hash)
                .add("head", head)
                .toString();
    }
}
