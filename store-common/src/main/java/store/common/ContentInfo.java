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

    private static final String CONTENT = "content";
    private static final String LENGTH = "length";
    private static final String REVISION = "revision";
    private static final String PARENT = "parent";
    private static final String PARENTS = "parents";
    private static final String DELETED = "deleted";
    private static final String METADATA = "metadata";
    private final Hash content;
    private final long length;
    private final Hash revision;
    private final SortedSet<Hash> parents;
    private final boolean deleted;
    private final Map<String, Value> metadata;

    private ContentInfo(ContentInfoBuilder builder) {
        content = requireNonNull(builder.content);
        length = requireNonNull(builder.length);
        revision = requireNonNull(builder.revision);
        parents = unmodifiableSortedSet(builder.parents);
        deleted = requireNonNull(builder.deleted);
        metadata = unmodifiableMap(builder.metadata);
    }

    /**
     * @return The associated content hash.
     */
    public Hash getContent() {
        return content;
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
    public Hash getRevision() {
        return revision;
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
                .put(CONTENT, content)
                .put(LENGTH, length)
                .put(REVISION, revision);

        switch (parents.size()) {
            case 0:
                break;
            case 1:
                builder.put(PARENT, parents.first());
                break;
            default:
                builder.put(PARENTS, toList(parents));
                break;
        }
        if (deleted) {
            builder.put(DELETED, deleted);
        }
        if (!metadata.isEmpty()) {
            builder.put(METADATA, metadata);
        }
        return builder.build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ContentInfo fromMap(Map<String, Value> map) {
        ContentInfoBuilder builder = new ContentInfoBuilder()
                .withContent(map.get(CONTENT).asHash())
                .withLength(map.get(LENGTH).asLong());

        if (map.containsKey(PARENT)) {
            builder.withParent(map.get(PARENT).asHash());
        }
        if (map.containsKey(PARENTS)) {
            builder.withParents(fromList(map.get(PARENTS).asList()));
        }
        if (map.containsKey(DELETED)) {
            builder.withDeleted(map.get(DELETED).asBoolean());
        }
        if (map.containsKey(METADATA)) {
            builder.withMetadata(map.get(METADATA).asMap());
        }
        return builder.build(map.get(REVISION).asHash());
    }

    @Override
    public int hashCode() {
        return Objects.hash(content,
                            length,
                            revision,
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
                .append(content, other.content)
                .append(length, other.length)
                .append(revision, other.revision)
                .append(parents, other.parents)
                .append(deleted, other.deleted)
                .append(metadata, other.metadata)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(CONTENT, content)
                .add(LENGTH, length)
                .add(REVISION, revision)
                .add(PARENTS, parents)
                .add(DELETED, deleted)
                .add(METADATA, metadata)
                .toString();
    }

    /**
     * Builder.
     */
    public static class ContentInfoBuilder {

        private Hash content;
        private Long length;
        private Hash revision;
        private final SortedSet<Hash> parents = new TreeSet<>();
        private boolean deleted;
        private final Map<String, Value> metadata = new TreeMap<>();

        /**
         * Set associated content hash.
         *
         * @param hash The associated content hash.
         * @return this
         */
        public ContentInfoBuilder withContent(Hash hash) {
            this.content = hash;
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
        public ContentInfo computeRevisionAndBuild() {
            BsonWriter writer = new BsonWriter()
                    .put(CONTENT, content.getBytes())
                    .put(LENGTH, content.getBytes());

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
                revision = IoUtil.copyAndDigest(new ByteArrayInputStream(writer.build()), sink()).getHash();
                return new ContentInfo(this);

            } catch (IOException e) {
                // Actually impossible.
                throw new AssertionError(e);
            }
        }

        /**
         * Build.
         *
         * @param revision The hash of this revision.
         * @return A new ContentInfo instance.
         */
        public ContentInfo build(Hash revision) {
            this.revision = revision;
            return new ContentInfo(this);
        }
    }
}
