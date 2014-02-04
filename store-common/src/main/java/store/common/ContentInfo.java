package store.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import static store.common.SinkOutputStream.sink;
import store.common.io.ObjectEncoder;
import store.common.value.Value;

public class ContentInfo {

    private final Hash hash;
    private final Hash rev;
    private final SortedSet<Hash> parents;
    private final boolean deleted;
    private final long length;
    private final Map<String, Value> metadata;

    private ContentInfo(ContentInfoBuilder builder) {
        hash = requireNonNull(builder.hash);
        rev = requireNonNull(builder.rev);
        parents = new UnmodifiableSortedSet<>(builder.parents);
        deleted = requireNonNull(builder.deleted);
        length = requireNonNull(builder.length);
        metadata = unmodifiableMap(builder.metadata);
    }

    public Hash getHash() {
        return hash;
    }

    public Hash getRev() {
        return rev;
    }

    public SortedSet<Hash> getParents() {
        return parents;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public long getLength() {
        return length;
    }

    public Map<String, Value> getMetadata() {
        return metadata;
    }

    public ContentInfo with(String key, Value value) {
        return new ContentInfoBuilder()
                .withHash(hash)
                .withLength(length)
                .withMetadata(metadata)
                .with(key, value)
                .build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, rev);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != ContentInfo.class) {
            return false;
        }
        ContentInfo that = (ContentInfo) obj;
        return rev.equals(that.rev);
    }

    /**
     * Builder.
     */
    public static class ContentInfoBuilder {

        private Hash hash;
        private Hash rev;
        private final SortedSet<Hash> parents = new TreeSet<>();
        private boolean deleted;
        private Long length;
        private final Map<String, Value> metadata = new TreeMap<>();

        public ContentInfoBuilder withHash(Hash hash) {
            this.hash = hash;
            return this;
        }

        public ContentInfoBuilder withRev(Hash rev) {
            this.rev = rev;
            return this;
        }

        public ContentInfoBuilder withParents(Set<Hash> parents) {
            this.parents.addAll(parents);
            return this;
        }

        public ContentInfoBuilder withParent(Hash parent) {
            parents.add(parent);
            return this;
        }

        public ContentInfoBuilder withDeleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public ContentInfoBuilder withLength(long length) {
            this.length = length;
            return this;
        }

        public ContentInfoBuilder withMetadata(Map<String, Value> metadata) {
            this.metadata.putAll(metadata);
            return this;
        }

        public ContentInfoBuilder with(String key, Value value) {
            this.metadata.put(key, value);
            return this;
        }

        public ContentInfo computeRevAndBuild() {
            ObjectEncoder encoder = new ObjectEncoder()
                    .put("hash", hash.value());

            List<Value> list = new ArrayList<>();
            for (Hash parent : parents) {
                list.add(Value.of(parent.value()));
            }
            encoder.put("parents", list);
            if (deleted) {
                encoder.put("deleted", deleted);
            }
            encoder.put("length", length);
            for (Entry<String, Value> entry : metadata.entrySet()) {
                encoder.put(entry.getKey(), entry.getValue());
            }
            try {
                rev = IoUtil.copyAndDigest(new ByteArrayInputStream(encoder.build()), sink()).getHash();
                return new ContentInfo(this);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public ContentInfo build() {
            return new ContentInfo(this);
        }
    }
}
