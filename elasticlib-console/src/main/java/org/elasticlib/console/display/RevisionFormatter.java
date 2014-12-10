/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
