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
 * Defines all possible agent states.
 */
public enum AgentState {

    /**
     * Agent has not been started yet.
     */
    NEW,
    /**
     * Agent is up and doing some work.
     */
    RUNNING,
    /**
     * Agent is up but waiting for more work.
     */
    WAITING,
    /**
     * Agent is down, due to an unexpected error.
     */
    ERROR,
    /**
     * Agent is down, because its source or target is unavailable.
     */
    STOPPED;

    /**
     * Provides state matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg A state as a string, as obtained by a call to toString().
     * @return Corresponding state.
     */
    public static AgentState fromString(String arg) {
        return AgentState.valueOf(arg.toUpperCase());
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
