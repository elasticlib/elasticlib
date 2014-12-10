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

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if an operation references an unknown repository.
 */
public final class UnknownRepositoryException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return NOT_FOUND;
    }

    @Override
    public String getMessage() {
        return "This repository is unknown";
    }
}
