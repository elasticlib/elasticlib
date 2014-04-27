package store.client.display;

import com.google.common.base.Function;
import store.common.ContentInfo;
import store.common.MapBuilder;
import store.common.value.Value;
import store.common.yaml.YamlWriting;

/**
 * Revision formatter for tree representation.
 */
class RevisionFormatter implements Function<ContentInfo, String> {

    private static final String REVISION = "revision";
    private static final String DELETED = "deleted";
    private static final String METADATA = "metadata";
    private final boolean prettyDisplay;

    public RevisionFormatter(boolean prettyDisplay) {
        this.prettyDisplay = prettyDisplay;
    }

    @Override
    public String apply(ContentInfo info) {
        MapBuilder builder = new MapBuilder()
                .put(REVISION, info.getRevision());

        if (info.isDeleted()) {
            builder.put(DELETED, true);
        }
        if (!info.getMetadata().isEmpty()) {
            builder.put(METADATA, info.getMetadata());
        }

        Value value = Value.of(builder.build());
        if (prettyDisplay) {
            value = MappableFormatting.formatValue(value);
        }
        return YamlWriting.writeValue(value);
    }
}
