package org.elasticlib.console.display;

import java.util.function.Function;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.yaml.YamlWriter;

/**
 * Revision formatter for tree representation.
 */
class RevisionFormatter implements Function<Revision, String> {

    private static final String REVISION = "revision";
    private static final String DELETED = "deleted";
    private static final String METADATA = "metadata";
    private final boolean prettyDisplay;

    public RevisionFormatter(boolean prettyDisplay) {
        this.prettyDisplay = prettyDisplay;
    }

    @Override
    public String apply(Revision rev) {
        MapBuilder builder = new MapBuilder()
                .put(REVISION, rev.getRevision());

        if (rev.isDeleted()) {
            builder.put(DELETED, true);
        }
        if (!rev.getMetadata().isEmpty()) {
            builder.put(METADATA, rev.getMetadata());
        }

        Value value = Value.of(builder.build());
        if (prettyDisplay) {
            value = MappableFormatting.formatValue(value);
        }
        return YamlWriter.writeToString(value);
    }
}