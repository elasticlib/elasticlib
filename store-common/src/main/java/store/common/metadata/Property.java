package store.common.metadata;

/**
 * A metadata property.
 */
public interface Property {

    /**
     * @return This property as a string key, intended for internal use (content's info, index...).
     */
    String key();

    /**
     *
     * @return This property as a string label, intended for human display.
     */
    String label();
}
