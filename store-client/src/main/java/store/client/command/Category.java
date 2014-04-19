package store.client.command;

/**
 * Define command categories.
 */
enum Category {

    CONNECTION,
    SERVER,
    REPOSITORY,
    CONFIG,
    MISC,;

    @Override
    public String toString() {
        String name = name();
        return name.charAt(0) + name.substring(1).toLowerCase();
    }
}
