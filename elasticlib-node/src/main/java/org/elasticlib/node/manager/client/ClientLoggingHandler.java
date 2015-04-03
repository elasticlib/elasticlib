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
package org.elasticlib.node.manager.client;

import static java.lang.System.lineSeparator;
import org.elasticlib.common.client.LoggingHandler;
import org.slf4j.Logger;

/**
 * Utility handler used to route node client logging messages to SLF4J.
 */
public class ClientLoggingHandler implements LoggingHandler {

    private final Logger logger;

    /**
     * Constructor.
     *
     * @param logger Underlying logger.
     */
    public ClientLoggingHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void logRequest(String message) {
        logger.info("Sending request{}{}", lineSeparator(), message);
    }

    @Override
    public void logResponse(String message) {
        logger.info("Received response{}{}", lineSeparator(), message);
    }
}
