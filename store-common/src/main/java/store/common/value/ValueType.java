package store.common.value;

/**
 * Enumerates possible types of a value.
 */
public enum ValueType {

    /**
     * Nothing.
     */
    NULL,
    /**
     * A byte array.
     */
    BINARY,
    /**
     * A boolean.
     */
    BOOLEAN,
    /**
     * A long.
     */
    LONG,
    /**
     * A big decimal.
     */
    BIG_DECIMAL,
    /**
     * A string.
     */
    STRING,
    /**
     * A date.
     */
    DATE,
    /**
     * A map of values.
     */
    OBJECT,
    /**
     * A list of values.
     */
    ARRAY,
}
