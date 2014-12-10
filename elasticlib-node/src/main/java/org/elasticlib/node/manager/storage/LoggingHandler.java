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
package org.elasticlib.node.manager.storage;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Storage manager logging handler. Publishes Berkeley DB internal log records to SLF4J logging.
 */
class LoggingHandler extends Handler {

    // Potential lost logger changes due to weak reference in OpenJDK: Hold a reference to prevent garbage collection.
    private static final Logger LOG = Logger.getLogger("com.sleepycat.je");

    static {
        // Bug in Berkeley DB 6.0.11 : NPE in Database.sync() if log level includes Level.FINEST.
        // Logging below this level is disabled to avoids this.
        LOG.setLevel(Level.FINER);
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
