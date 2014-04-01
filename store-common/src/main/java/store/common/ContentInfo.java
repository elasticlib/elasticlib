package store.common;

import static com.google.common.base.Objects.toStringHelper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import static store.common.MappableUtil.fromList;
import static store.common.MappableUtil.toList;
import static store.common.SinkOutputStream.sink;
import store.common.bson.BsonWriter;
import store.common.value.Value;

/**
 * Represents a revision of metadata about a content.
 */
public class ContentInfo implements Mappable {

    private static final String HASH = "hash";
    private static final String LENGTH = "length";
    private static final String REV = "rev";
    private static final String PARENTS = "parents";
    private static final String DELETED = "deleted";
    private static final String METADATA = "metadata";
    private final Hash hash;
    private final long length;
    private final Hash rev;
    private final SortedSet<Hash> parents;
    private final boolean deleted;
    private final Map<String, Value> metadata;

    private ContentInfo(ContentInfoBuilder builder) {
        hash = requireNonNull(builder.hash);
        length = requireNonNull(builder.length);
        rev = requireNonNull(builder.rev);
        parents = unmodifiableSortedSet(builder.parents);
        deleted = requireNonNull(builder.deleted);
        metadata = unmodifiableMap(builder.metadata);
    }

    /**
     * @return The associated content hash.
     */
    public Hash getHash() {
        return hash;
    }

    /**
     * @return The associated content length.
     */
    public long getLength() {
        return length;
    }

    /**
     * @return The hash of this revision.
     */
    public Hash getRev() {
        return rev;
    }

    /**
     * @return Hashes of parents revisions.
     */
    public SortedSet<Hash> getParents() {
        return parents;
    }

    /**
     * @return true if associated content has actually been deleted.
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * @return Metadata attached to this revision.
     */
    public Map<String, Value> getMetadata() {
        return metadata;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(HASH, hash)
                .put(LENGTH, length)
                .put(REV, rev)
                .put(PARENTS, toList(parents));

        if (deleted) {
            builder.put(DELETED, deleted);
        }
        return builder.put(METADATA, metadata).build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ContentInfo fromMap(Map<String, Value> map) {
        ContentInfoBuilder builder = new ContentInfoBuilder()
                .withHash(new Hash(map.get(HASH).asByteArray()))
                .withLength(map.get(LENGTH).asLong())
                .withParents(fromList(map.get(PARENTS).asList()))
                .withMetadata(map.get(METADATA).asMap());

        if (map.containsKey(DELETED)) {
            builder.withDeleted(map.get(DELETED).asBoolean());
        }
        return builder.build(new Hash(map.get(REV).asByteArray()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash,
                            length,
                            rev,
                            parents,
                            deleted,
                            metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ContentInfo)) {
            return false;
        }
        ContentInfo other = (ContentInfo) obj;
        return new EqualsBuilder()
                .append(hash, other.hash)
                .append(length, other.length)
                .append(rev, other.rev)
                .append(parents, other.parents)
                .append(deleted, other.deleted)
                .append(metadata, other.metadata)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(HASH, hash)
                .add(LENGTH, length)
                .add(REV, rev)
                .add(PARENTS, parents)
                .add(DELETED, deleted)
                .add(METADATA, metadata)
                .toString();
    }

    /**
     * Builder.
     */
    public static class ContentInfoBuilder {

        private Hash hash;
        private Long length;
        private Hash rev;
        private final SortedSet<Hash> parents = new TreeSet<>();
        private boolean deleted;
        private final Map<String, Value> metadata = new TreeMap<>();

        /**
         * Set associated content hash.
         *
         * @param hash The associated content hash.
         * @return this
         */
        public ContentInfoBuilder withHash(Hash hash) {
            this.hash = hash;
            return this;
        }

        /**
         * Set associated content length.
         *
         * @param length The associated content length.
         * @return this
         */
        public ContentInfoBuilder withLength(long length) {
            this.length = length;
            return this;
        }

        /**
         * Add parent revisions.
         *
         * @param parents Hashes of parents revisions to add.
         * @return this
         */
        public ContentInfoBuilder withParents(Set<Hash> parents) {
            this.parents.addAll(parents);
            return this;
        }

        /**
         * Add a parent revision.
         *
         * @param parent parent revision to add.
         * @return this
         */
        public ContentInfoBuilder withParent(Hash parent) {
            parents.add(parent);
            return this;
        }

        /**
         * Set content deletion status.
         *
         * @param deleted true if associated content has actually been deleted.
         * @return this
         */
        public ContentInfoBuilder withDeleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        /**
         * Add metadata.
         *
         * @param metadata Metadata.
         * @return this
         */
        public ContentInfoBuilder withMetadata(Map<String, Value> metadata) {
            this.metadata.putAll(metadata);
            return this;
        }

        /**
         * Add a key-value pair to metadata.
         *
         * @param key Metadata key.
         * @param value Associated value.
         * @return this
         */
        public ContentInfoBuilder with(String key, Value value) {
            this.metadata.put(key, value);
            return this;
        }

        /**
         * Compute revision hash and build.
         *
         * @return A new ContentInfo instance.
         */
        public ContentInfo computeRevAndBuild() {
            BsonWriter writer = new BsonWriter()
                    .put(HASH, hash.getBytes())
                    .put(LENGTH, hash.getBytes());

            List<Value> list = new ArrayList<>();
            for (Hash parent : parents) {
                list.add(Value.of(parent.getBytes()));
            }
            writer.put(PARENTS, list);
            if (deleted) {
                writer.put(DELETED, deleted);
            }
            writer.put(METADATA, metadata);
            try {
                rev = IoUtil.copyAndDigest(new ByteArrayInputStream(writer.build()), sink()).getHash();
                return new ContentInfo(this);

            } catch (IOException e) {
                // Actually impossible.
                throw new AssertionError(e);
            }
        }

        /**
         * Build.
         *
         * @param rev The hash of this revision.
         * @return A new ContentInfo instance.
         */
        public ContentInfo build(Hash rev) {
            this.rev = rev;
            return new ContentInfo(this);
        }
    }
}
