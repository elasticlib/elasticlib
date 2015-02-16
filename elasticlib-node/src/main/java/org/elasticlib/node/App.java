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
package org.elasticlib.node;

import static java.lang.Runtime.getRuntime;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Node app.
 */
public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private static Node node;
    private static volatile CountDownLatch keepAliveLatch;
    private static volatile Thread keepAliveThread;

    private App() {
    }

    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            LOG.error("The path to this node home-directory has to be supplied in the command line arguments");
            return;
        }

        // Optionally remove existing handlers and add SLF4JBridgeHandler to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        Path home = Paths.get(args[0]);
        node = new Node(home);
        node.start();

        getRuntime().addShutdownHook(new Thread(() -> {
            node.stop();
            keepAliveLatch.countDown();

        }, "shutdown"));

        keepAliveLatch = new CountDownLatch(1);
        keepAliveThread = new Thread(() -> {
            try {
                keepAliveLatch.await();

            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }, "keepAlive");
        keepAliveThread.setDaemon(false);
        keepAliveThread.start();
    }

    /**
     * Hook used by Prunsrv to stop this app.
     *
     * @param args Command line arguments.
     */
    public static void close(String[] args) {
        node.stop();
        keepAliveLatch.countDown();
    }
}
