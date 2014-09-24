package store.client.http;

import store.client.config.ClientConfig;
import store.client.display.Display;
import store.common.client.LoggingHandler;

/**
 * A logging handler that prints log messages to display.
 */
class PrintingHandler implements LoggingHandler {

    private final Display display;
    private final ClientConfig config;
    private boolean enabled;

    /**
     * Constructor.
     *
     * @param display Display.
     * @param config Config.
     */
    public PrintingHandler(Display display, ClientConfig config) {
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
    public void log(String message) {
        if (enabled && config.isDisplayHttp()) {
            display.print(message);
        }
    }
}
