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
    BYTE_ARRAY,
    /**
     * A boolean.
     */
    BOOLEAN,
    /**
     * A byte.
     */
    BYTE,
    /**
     * An integer.
     */
    INTEGER,
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
    MAP,
    /**
     * A list of values.
     */
    LIST,
}
