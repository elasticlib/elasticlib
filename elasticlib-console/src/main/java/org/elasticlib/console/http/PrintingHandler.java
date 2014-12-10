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
package org.elasticlib.console.http;

import org.elasticlib.common.client.LoggingHandler;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;

/**
 * A logging handler that prints log messages to display.
 */
class PrintingHandler implements LoggingHandler {

    private final Display display;
    private final ConsoleConfig config;
    private boolean enabled;

    /**
     * Constructor.
     *
     * @param display Display.
     * @param config Config.
     */
    public PrintingHandler(Display display, ConsoleConfig config) {
        this.display = display;
        this.config = config;
    }

    /**
     * Turns on/off this handler.
     *
     * @param val If this handler should be actived.
     */
    public void setEnabled(boolean val) {
        enabled = val;
    }

    @Override
    public void logRequest(String message) {
        log(message);
    }

    @Override
    public void logResponse(String message) {
        log(message);
    }

    private void log(String message) {
        if (enabled && config.isDisplayHttp()) {
            display.println(message);
        }
    }
}
