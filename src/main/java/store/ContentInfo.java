package store;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import store.hash.Hash;

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
