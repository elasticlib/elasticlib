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

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if a remote operation fails because target node is unreachable.
 */
public final class UnreachableNodeException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return SERVICE_UNAVAILABLE;
    }

    /**
     * Constructor.
     */
    public UnreachableNodeException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception to wrap.
     */
    public UnreachableNodeException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Remote node is not reachable";
    }
}
