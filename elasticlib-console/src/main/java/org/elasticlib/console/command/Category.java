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
 * Define command categories.
 */
public enum Category {

    /**
     * Current node related commands.
     */
    NODE,
    /**
     * Remote nodes related commands.
     */
    REMOTES,
    /**
     * Repositories related commands.
     */
    REPOSITORIES,
    /**
     * Replications related commands.
     */
    REPLICATIONS,
    /**
     * Current repository contents related commands.
     */
    CONTENTS,
    /**
     * Console configuration related commands.
     */
    CONFIG,
    /**
     * Miscellaneous commands
     */
    MISC,;

    @Override
    public String toString() {
        String name = name();
        return name.charAt(0) + name.substring(1).toLowerCase();
    }
}
