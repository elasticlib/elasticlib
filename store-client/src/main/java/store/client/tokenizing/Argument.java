package store.client.tokenizing;

/**
 * Represents a command line argument.
 */
final class Argument {

    private final String value;
    private final int position;

    public Argument(String value, int position) {
        this.value = value;
        this.position = position;
    }

    public String getValue() {
        return value;
    }

    public int getPosition() {
        return position;
    }
}
