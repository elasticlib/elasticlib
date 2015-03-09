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

import java.io.Closeable;
import java.net.URI;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.client.RepositoryClient;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.RequestFailedException;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private static final String NOT_CONNECTED = "Not connected";
    private static final String NO_REPOSITORY = "No repository selected";

    private final ConsoleConfig config;
    private final PrintingHandler printingHandler;
    private Client client;
    private String node;
    private RepositoryDef repositoryDef;

    /**
     * Constructor.
     *
     * @param display Display.
     * @param config Config.
     */
    public Session(Display display, ConsoleConfig config) {
        this.config = config;
        printingHandler = new PrintingHandler(display, config);
    }

    /**
     * Initialisation. Set default connection, if any.
     */
    public void init() {
        if (config.getDefaultNode() == null) {
            return;
        }
        connect(config.getDefaultNode());
        if (config.getDefaultRepository().isEmpty()) {
            return;
        }
        use(config.getDefaultRepository());
    }

    /**
     * Connects to node at supplied URI.
     *
     * @param uri Node URI.
     */
    public void connect(URI uri) {
        close();
        client = new Client(uri, printingHandler);
        printingHandler.setEnabled(true);
        try {
            node = client.node().getInfo().getName();

        } finally {
            printingHandler.setEnabled(false);
        }
    }

    /**
     * Set if HTTP dialog should be printed.
     *
     * @param val If this feature should be activated.
     */
    public void printHttpDialog(boolean val) {
        printingHandler.setEnabled(val);
    }

    /**
     * Disconnects from current server. Idempotent.
     */
    public void disconnect() {
        close();
    }

    /**
     * Selects a repository to use.
     *
     * @param repository Repository name or encoded GUID.
     */
    public void use(String repository) {
        if (client == null) {
            throw new RequestFailedException(NOT_CONNECTED);
        }
        printingHandler.setEnabled(true);
        try {
            RepositoryDef def = client.repositories()
                    .get(repository)
                    .getInfo()
                    .getDef();

            repositoryDef = def;
        } finally {
            printingHandler.setEnabled(false);
        }
    }

    /**
     * Stops using current repository, if any.
     */
    public void leave() {
        if (repositoryDef != null) {
            repositoryDef = null;
        }
    }

    /**
     * Stops using repository which GUID is supplied, if applicable. Does nothing otherwise.
     *
     * @param repositoryGuid Repository GUID.
     */
    public void leave(Guid repositoryGuid) {
        if (repositoryDef != null && repositoryDef.getGuid().equals(repositoryGuid)) {
            leave();
        }
    }

    /**
     * Provides current node client. Fails if there is currently no alive connection.
     *
     * @return A node client.
     */
    public Client getClient() {
        if (client == null) {
            throw new RequestFailedException(NOT_CONNECTED);
        }
        return client;
    }

    /**
     * Provides a client on current repository. Fails if there is currently no selected repository.
     *
     * @return A repository client.
     */
    public RepositoryClient getRepository() {
        if (client == null) {
            throw new RequestFailedException(NOT_CONNECTED);
        }
        if (repositoryDef == null) {
            throw new RequestFailedException(NO_REPOSITORY);
        }
        return client.repositories().get(repositoryDef.getGuid());
    }

    /**
     * Provides a string which describes the connection to the current node and repository, if any. Returns an empty
     * string if there is currently no connection.
     *
     * @return A string describing the current connection.
     */
    public String getConnectionString() {
        if (node == null) {
            return "";
        }
        if (repositoryDef == null) {
            return node;
        }
        return String.join("/", node, repositoryDef.getName());
    }

    @Override
    public void close() {
        node = null;
        repositoryDef = null;
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
