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
