package store.client.http;

import com.google.common.base.Joiner;
import java.io.Closeable;
import java.net.URI;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private static final String NOT_CONNECTED = "Not connected";
    private static final String NO_REPOSITORY = "No repository selected";
    private final Display display;
    private final ClientConfig config;
    private HttpClient restClient;
    private String server;
    private String repository;

    /**
     * Constructor.
     *
     * @param display Display.
     * @param config Config.
     */
    public Session(Display display, ClientConfig config) {
        this.display = display;
        this.config = config;

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
        restClient = new HttpClient(uri, display, config);
        restClient.printHttpDialog(true);
        try {
            server = restClient.getNode().getName();
            display.setPrompt(server);

        } finally {
            restClient.printHttpDialog(false);
        }
    }

    /**
     * Set if HTTP dialog should be printed.
     *
     * @param val If this feature should be activated.
     */
    public void printHttpDialog(boolean val) {
        if (restClient != null) {
            restClient.printHttpDialog(val);
        }
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
        restClient.printHttpDialog(true);
        try {
            restClient.testRepository(repositoryName);
            display.setPrompt(Joiner.on('/').join(server, repositoryName));
            repository = repositoryName;
        } finally {
            restClient.printHttpDialog(false);
        }
    }

    /**
     * Stop using current repository, if any.
     */
    public void leave() {
        if (repository != null) {
            repository = null;
            if (server == null) {
                display.resetPrompt();
            } else {
                display.setPrompt(server);
            }
        }
    }

    /**
     * Stop using repository which name is supplied, if applicable. Does nothing otherwise.
     *
     * @param repositoryName Repository name.
     */
    public void leave(String repositoryName) {
        if (repository != null && repository.equals(repositoryName)) {
            leave();
        }
    }

    /**
     * Provides current repository name. Fails if there is currently no selected repository.
     *
     * @return A repository name.
     */
    public String getRepository() {
        if (restClient == null) {
            throw new RequestFailedException(NOT_CONNECTED);
        }
        if (repository == null) {
            throw new RequestFailedException(NO_REPOSITORY);
        }
        return repository;
    }

    /**
     * Provides current HTTP client. Fails if there is currently no alive connection.
     *
     * @return A HTTP client.
     */
    public HttpClient getClient() {
        if (restClient == null) {
            throw new RequestFailedException(NOT_CONNECTED);
        }
        return restClient;
    }

    @Override
    public void close() {
        server = null;
        repository = null;
        display.resetPrompt();
        if (restClient != null) {
            restClient.close();
            restClient = null;
        }
    }
}
