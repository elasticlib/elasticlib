package store.client.http;

import com.google.common.base.Joiner;
import java.io.Closeable;
import java.net.URI;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.common.client.Client;
import store.common.client.RepositoryClient;
import store.common.hash.Guid;
import store.common.model.RepositoryDef;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private static final String NOT_CONNECTED = "Not connected";
    private static final String NO_REPOSITORY = "No repository selected";
    private final Display display;
    private final ClientConfig config;
    private final PrintingHandler printingHandler;
    private Client client;
    private String node;
    private Guid repository;

    /**
     * Constructor.
     *
     * @param display Display.
     * @param config Config.
     */
    public Session(Display display, ClientConfig config) {
        this.display = display;
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
            node = client.node().getDef().getName();
            display.setPrompt(node);

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
     * @param repositoryName Repository name.
     */
    public void use(String repositoryName) {
        if (client == null) {
            throw new RequestFailedException(NOT_CONNECTED);
        }
        printingHandler.setEnabled(true);
        try {
            RepositoryDef def = client.repositories()
                    .getInfo(repositoryName)
                    .getDef();

            repository = def.getGuid();
            display.setPrompt(Joiner.on('/').join(node, def.getName()));
        } finally {
            printingHandler.setEnabled(false);
        }
    }

    /**
     * Stops using current repository, if any.
     */
    public void leave() {
        if (repository != null) {
            repository = null;
            if (node == null) {
                display.resetPrompt();
            } else {
                display.setPrompt(node);
            }
        }
    }

    /**
     * Stops using repository which GUID is supplied, if applicable. Does nothing otherwise.
     *
     * @param repositoryGuid Repository GUID.
     */
    public void leave(Guid repositoryGuid) {
        if (repository != null && repository.equals(repositoryGuid)) {
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
        if (repository == null) {
            throw new RequestFailedException(NO_REPOSITORY);
        }
        return client.repositories().get(repository);
    }

    @Override
    public void close() {
        node = null;
        repository = null;
        display.resetPrompt();
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
