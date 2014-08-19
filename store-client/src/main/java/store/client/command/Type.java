package store.client.command;

/**
 * Define command line parameter types.
 */
enum Type {

    /**
     * A config key.
     */
    KEY,
    /**
     * A config value.
     */
    VALUE,
    /**
     * A node URI.
     */
    URI,
    /**
     * A file-system path.
     */
    PATH,
    /**
     * A file-system directory.
     */
    DIRECTORY,
    /**
     * A node name or GUID.
     */
    NODE,
    /**
     * A repository name or GUID.
     */
    REPOSITORY,
    /**
     * A hash.
     */
    HASH,
    /**
     * A query.
     */
    QUERY,
    /**
     * A command name.
     */
    COMMAND;
}
