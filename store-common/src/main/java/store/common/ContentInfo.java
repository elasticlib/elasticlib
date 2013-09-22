package store.common;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import store.common.Hash;

public final class ContentInfo {

    private final Hash hash;
    private final long length;
    private final Map<String, Object> metadata;

    private ContentInfo(ContentInfoBuilder builder) {
        hash = requireNonNull(builder.hash);
        length = requireNonNull(builder.length);
        metadata = requireNonNull(unmodifiableMap(builder.metadata));
    }

    public Hash getHash() {
        return hash;
    }

    public long getLength() {
        return length;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public static ContentInfoBuilder contentInfo() {
        return new ContentInfoBuilder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, length, metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != ContentInfo.class) {
            return false;
        }
        final ContentInfo other = (ContentInfo) obj;
        if (!hash.equals(other.hash)) {
            return false;
        }
        if (length != other.length) {
            return false;
        }
        if (!metadata.equals(other.metadata)) {
            return false;
        }
        return true;
    }

    /**
     * Builder
     */
    public static class ContentInfoBuilder {

        private Hash hash;
        private Long length;
        private final Map<String, Object> metadata = new HashMap<>();

        private ContentInfoBuilder() {
        }

        public ContentInfoBuilder withHash(Hash hash) {
            this.hash = hash;
            return this;
        }

        public ContentInfoBuilder withLength(long length) {
            this.length = length;
            return this;
        }

        public ContentInfoBuilder withMetadata(Map<String, Object> metadata) {
            this.metadata.putAll(metadata);
            return this;
        }

        public ContentInfo build() {
            return new ContentInfo(this);
        }
    }
}
