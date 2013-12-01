package store.client;

/**
 * Define command line parameter types.
 */
public enum Type {

    /**
     * A file-system path.
     */
    PATH,
    /**
     * A volume name.
     */
    VOLUME,
    /**
     * An index name.
     */
    INDEX,
    /**
     * A hash.
     */
    HASH,
    /**
     * A query.
     */
    QUERY;
}
