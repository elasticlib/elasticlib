package org.elasticlib.common.model;

/**
 * Defines all possible content states.
 */
public enum ContentState {

    /**
     * Content does not exist or has been deleted.
     */
    ABSENT,
    /**
     * Content has been partially staged.
     */
    PARTIAL,
    /**
     * Staging of content is currently happening.
     */
    STAGING,
    /**
     * Content has been fully staged.
     */
    STAGED,
    /**
     * Content is present.
     */
    PRESENT;

    /**
     * Provides state matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg A state as a string, as obtained by a call to toString().
     * @return Corresponding state.
     */
    public static ContentState fromString(String arg) {
        return valueOf(arg.toUpperCase());
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
