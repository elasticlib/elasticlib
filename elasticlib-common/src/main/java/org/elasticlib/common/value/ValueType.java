package org.elasticlib.common.value;

/**
 * Enumerates possible types of a value.
 */
public enum ValueType {

    /**
     * Nothing.
     */
    NULL,
    /**
     * A hash.
     */
    HASH,
    /**
     * A GUID.
     */
    GUID,
    /**
     * Some binary data.
     */
    BINARY,
    /**
     * A boolean.
     */
    BOOLEAN,
    /**
     * An integral number.
     */
    INTEGER,
    /**
     * A decimal number.
     */
    DECIMAL,
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
