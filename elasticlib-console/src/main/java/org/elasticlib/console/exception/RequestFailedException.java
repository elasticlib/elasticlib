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
package org.elasticlib.console.exception;

/**
 * Thrown if a requests fails for any reason.
 */
public class RequestFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public RequestFailedException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param e The cause of this exception.
     */
    public RequestFailedException(Exception e) {
        super(e.getMessage());
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     * @param e The cause of this exception.
     */
    public RequestFailedException(String message, Exception e) {
        super(message + System.lineSeparator() + e.getMessage());
    }
}
