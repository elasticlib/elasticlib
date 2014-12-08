package org.elasticlib.console.config;

/**
 * Defines available rendering formats.
 */
public enum Format {

    /**
     * The YAML format.
     */
    YAML,
    /**
     * The JSON format.
     */
    JSON;

    /**
     * Checks if supplied argument correspond to a valid format.
     *
     * @param arg A format as a string, as obtained by a call to toString().
     * @return If a corresponding format exists.
     */
    public static boolean isSupported(String arg) {
        String upper = arg.toUpperCase();
        for (Format format : values()) {
            if (format.name().equals(upper)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Provides format matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg A format as a string, as obtained by a call to toString().
     * @return Corresponding format.
     */
    public static Format fromString(String arg) {
        return Format.valueOf(arg.toUpperCase());
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
