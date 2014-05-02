package store.common.metadata;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

/**
 * Enumerates properties.
 */
public final class Properties {

    private Properties() {
    }

    /**
     * Common properties.
     */
    public static enum Common implements Property {

        /**
         * File name.
         */
        FILE_NAME,
        /**
         * Content type.
         */
        CONTENT_TYPE,
        /**
         * File path (may be relative).
         */
        PATH;

        @Override
        public String key() {
            return key(name());
        }
    }

    /**
     * Text properties.
     */
    public static enum Text implements Property {

        /**
         * Encoding.
         */
        ENCODING;

        @Override
        public String key() {
            return key(name());
        }
    }

    /**
     * Audio properties.
     */
    public static enum Audio implements Property {

        /**
         * Artist.
         */
        ARTIST,
        /**
         * Album.
         */
        ALBUM,
        /**
         * Title.
         */
        TITLE,
        /**
         * Genre.
         */
        GENRE,
        /**
         * Track number.
         */
        TRACK_NUMBER,
        /**
         * Album duration.
         */
        DURATION,
        /**
         * Album release date.
         */
        RELEASE_DATE;

        @Override
        public String key() {
            return key(name());
        }
    }

    /**
     * Image properties.
     */
    public static enum Image implements Property {

        /**
         * Image height.
         */
        HEIGHT,
        /**
         * Image width.
         */
        WIDTH,
        /**
         * Image original date.
         */
        ORIGINAL_DATE;

        @Override
        public String key() {
            return key(name());
        }
    }

    private static String key(String name) {
        return UPPER_UNDERSCORE.to(LOWER_CAMEL, name);
    }
}
