/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.metadata;

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
            return toLowerCamel(name());
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
            return toLowerCamel(name());
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
            return toLowerCamel(name());
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
            return toLowerCamel(name());
        }
    }

    private static String toLowerCamel(String name) {
        return UPPER_UNDERSCORE.to(LOWER_CAMEL, name);
    }
}
