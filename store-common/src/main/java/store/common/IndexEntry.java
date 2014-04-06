package store.common;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Map;
import static java.util.Objects.hash;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import static store.common.MappableUtil.revisions;
import store.common.hash.Hash;
import store.common.value.Value;

/**
 * Represents an entry about a content in an index.
 */
public final class IndexEntry implements Mappable {

    private static final String CONTENT = "content";
    private static final String REVISIONS = "revisions";
    private final Hash content;
    private final SortedSet<Hash> revisions;

    /**
     * Constructor.
     *
     * @param content Content hash.
     * @param revisions Hashes of the content head revisions.
     */
    public IndexEntry(Hash content, Set<Hash> revisions) {
        this.content = content;
        this.revisions = unmodifiableSortedSet(new TreeSet<>(revisions));
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
    public SortedSet<Hash> getRevisions() {
        return revisions;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(CONTENT, content)
                .putRevisions(revisions)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static IndexEntry fromMap(Map<String, Value> map) {
        return new IndexEntry(map.get(CONTENT).asHash(), revisions(map));
    }

    @Override
    public int hashCode() {
        return hash(content, revisions);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexEntry)) {
            return false;
        }
        IndexEntry other = (IndexEntry) obj;
        return new EqualsBuilder()
                .append(content, other.content)
                .append(revisions, other.revisions)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(CONTENT, content)
                .add(REVISIONS, revisions)
                .toString();
    }
}
