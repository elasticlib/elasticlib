package store.client.command;

/**
 * Define command categories.
 */
enum Category {

    NODE,
    REMOTES,
    REPOSITORIES,
    REPLICATIONS,
    CONTENTS,
    CONFIG,
    MISC,;

    @Override
    public String toString() {
        String name = name();
        return name.charAt(0) + name.substring(1).toLowerCase();
    }
}
