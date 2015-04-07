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
package org.elasticlib.common.model;

/**
 * Defines replication types.
 */
public enum ReplicationType {

    /**
     * Source and destination repositories on the local node.
     */
    LOCAL,
    /**
     * Source repository on the local node and destination repository on a remote one.
     */
    PUSH,
    /**
     * Source repository on a remote node and destination repository on the local one.
     */
    PULL,
    /**
     * Source and destination repositories on remote nodes.
     */
    REMOTE;

    /**
     * Provides replication type matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg A type as a string, as obtained by a call to toString().
     * @return Corresponding type.
     */
    public static ReplicationType fromString(String arg) {
        return ReplicationType.valueOf(arg.toUpperCase());
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
