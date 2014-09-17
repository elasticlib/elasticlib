package store.server.manager.storage;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Storage manager logging handler. Publishes Berkeley DB internal log records to SLF4J logging.
 */
class LoggingHandler extends Handler {

    static {
        // Bug in Berkeley DB 6.0.11 : NPE in Database.sync() if log level includes Level.FINEST.
        // Logging below this level is disabled to avoids this.
        Logger.getLogger("com.sleepycat.je").setLevel(Level.FINER);
    }
    private final Handler delegate = new SLF4JBridgeHandler();
    private final String name;

    public LoggingHandler(String name) {
        this.name = name;
    }

    @Override
    public void publish(LogRecord record) {
        if (record == null || record.getMessage() == null) {
            return;
        }
        record.setMessage(prependName(record.getMessage()));
        delegate.publish(record);
    }

    private String prependName(String message) {
        return new StringBuilder()
                .append('[')
                .append(name)
                .append(']')
                .append(message.startsWith(" ") ? "" : " ")
                .append(message)
                .toString();
    }

    @Override
    public void flush() {
        // Nothing to do.
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
