package store.common.yaml;

import org.yaml.snakeyaml.nodes.Tag;

/**
 * Non-standard YAML tags.
 */
final class Tags {

    static final Tag HASH = new Tag("!hash");
    static final Tag GUID = new Tag("!guid");

    private Tags() {
    }
}
