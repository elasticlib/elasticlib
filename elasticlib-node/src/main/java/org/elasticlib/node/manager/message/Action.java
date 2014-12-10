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
package org.elasticlib.node.manager.message;

/**
 * Represents an action to apply when a message is posted.
 *
 * @param <T> Type of the message this action is associated to.
 */
public interface Action<T> {

    /**
     * Provides a short description of this action, intended for logging purposes.
     *
     * @return A description of this action.
     */
    String description();

    /**
     * Apply this action.
     *
     * @param message The message sent.
     */
    void apply(T message);
}
