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
package org.elasticlib.console.command;

/**
 * Define command line parameter types.
 */
public enum Type {

    /**
     * A config key.
     */
    KEY,
    /**
     * A config value.
     */
    VALUE,
    /**
     * A node URI.
     */
    URI,
    /**
     * A file-system path.
     */
    PATH,
    /**
     * A file-system directory.
     */
    DIRECTORY,
    /**
     * A node name or GUID.
     */
    NODE,
    /**
     * A repository name or GUID.
     */
    REPOSITORY,
    /**
     * A replication GUID.
     */
    REPLICATION,
    /**
     * A hash.
     */
    HASH,
    /**
     * A query.
     */
    QUERY,
    /**
     * A help subject.
     */
    SUBJECT;

    @Override
    public String toString() {
        return name();
    }
}
