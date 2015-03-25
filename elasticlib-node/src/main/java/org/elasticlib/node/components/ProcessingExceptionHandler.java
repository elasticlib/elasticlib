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
package org.elasticlib.node.components;

import java.net.SocketException;
import java.net.URI;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;

/**
 * Utility handler used to properly log instances of ProcessingException, thrown by client HTTP requests.
 */
public class ProcessingExceptionHandler {

    private final Logger logger;

    /**
     * Constructor.
     *
     * @param logger Underlying logger.
     */
    public ProcessingExceptionHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * Logs the supplied exception.
     *
     * @param target URI of the requested target.
     * @param exception Exception which happened.
     */
    public void log(URI target, ProcessingException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof SocketException) {
            logger.warn("Failed to request {} - {}: {}",
                        target,
                        cause.getClass().getSimpleName(),
                        cause.getMessage());
        } else {
            logger.warn("Failed to request " + target, exception);
        }
    }
}
