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
