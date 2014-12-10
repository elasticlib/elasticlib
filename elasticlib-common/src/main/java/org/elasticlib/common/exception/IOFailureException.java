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
package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if a IO error occurs on a node.
 */
public class IOFailureException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public IOFailureException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception to wrap.
     */
    public IOFailureException(Throwable cause) {
        super(message(cause), cause);
    }

    @Override
    public StatusType getStatus() {
        return INTERNAL_SERVER_ERROR;
    }
}
