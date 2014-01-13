package store.client.command;

/**
 * Define command line parameter types.
 */
enum Type {

    /**
     * A file-system path.
     */
    PATH,
    /**
     * A repository name.
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
