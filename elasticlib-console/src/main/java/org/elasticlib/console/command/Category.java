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
    NODE("Current node related commands"),
    /**
     * Remote nodes related commands.
     */
    REMOTES("Remote nodes related commands"),
    /**
     * Repositories related commands.
     */
    REPOSITORIES("Repositories related commands"),
    /**
     * Replications related commands.
     */
    REPLICATIONS("Replications related commands"),
    /**
     * Current repository contents related commands.
     */
    CONTENTS("Current repository contents related commands"),
    /**
     * Console configuration related commands.
     */
    CONFIG("Console configuration related commands"),
    /**
     * Miscellaneous commands.
     */
    MISC("Miscellaneous commands");

    private final String summary;

    private Category(String summary) {
        this.summary = summary;
    }

    /**
     * @return A short summary of this category.
     */
    public String summary() {
        return summary;
    }

    @Override
    public String toString() {
        String name = name();
        return name.charAt(0) + name.substring(1).toLowerCase();
    }
}
