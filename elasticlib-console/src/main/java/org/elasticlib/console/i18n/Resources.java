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
package org.elasticlib.console.i18n;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Splitter;
import static java.lang.System.lineSeparator;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;

/**
 * Provides text resources.
 */
public final class Resources {

    private Resources() {
    }

    /**
     * Provides resource corresponding to supplied key. Fails if it does not exist.
     *
     * @param key Resource key.
     * @return Corresponding resource value.
     */
    public static String get(String key) {
        Key parts = new Key(key);
        return ResourceBundle.getBundle(parts.getBundle())
                .getString(parts.getResource())
                .replace("\n", lineSeparator());
    }

    /**
     * Provides resource corresponding to supplied key, if any.
     *
     * @param key Resource key.
     * @return Corresponding resource value, if any.
     */
    public static Optional<String> tryGet(String key) {
        Key parts = new Key(key);
        ResourceBundle bundle = ResourceBundle.getBundle(parts.getBundle());
        if (!bundle.containsKey(parts.getResource())) {
            return Optional.empty();
        }
        String value = bundle.getString(parts.getResource()).replace("\n", lineSeparator());
        return Optional.of(value);
    }

    /**
     * Represents a resource access key. Immutable.
     */
    private static final class Key {

        private final String bundle;
        private final String resource;

        /**
         * Constructor.
         *
         * @param key Raw value of this key
         */
        public Key(String key) {
            List<String> parts = Splitter.on('.').limit(2).splitToList(key);
            checkArgument(parts.size() == 2, key);

            bundle = path(parts.get(0));
            resource = parts.get(1);
        }

        private static String path(String key) {
            String directory = Resources.class.getPackage().getName().replace('.', '/');
            return String.join("/", directory, key);
        }

        /**
         * @return The bundle name of the resource.
         */
        public String getBundle() {
            return bundle;
        }

        /**
         * @return The key of the resource itself.
         */
        public String getResource() {
            return resource;
        }
    }
}
