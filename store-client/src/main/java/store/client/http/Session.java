package store.client.http;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.io.Closeable;
import java.util.List;
import store.client.display.Display;
import store.client.exception.RequestFailedException;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private static final String NOT_CONNECTED = "Not connected";
    private static final String NO_REPOSITORY = "No repository selected";
    private static final String HTTP_SCHEME = "http://";
    private final Display display;
    private HttpClient restClient;
    private String server;
    private String repository;

    /**
     * Constructor.
     *
     * @param display Display.
     */
    public Session(Display display) {
        this.display = display;
    }

    /**
     * Connects to server at supplied URL. If URL contains a path fragment, it is interpreted as a repository name.
     *
     * @param url Server URL.
     */
    public void connect(String url) {
        close();
        url = url.trim();
        if (url.startsWith(HTTP_SCHEME)) {
            url = url.substring(HTTP_SCHEME.length());
        }
        List<String> parts = Splitter.on('/').trimResults().omitEmptyStrings().splitToList(url);
        if (parts.size() > 2) {
            throw new RequestFailedException("Malformed URL");
        }
        server = parts.get(0);
        restClient = new HttpClient(HTTP_SCHEME + server, display);
        if (parts.size() == 2) {
            use(parts.get(1));
        } else {
            restClient.testConnection();
            display.setPrompt(server);
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
        restClient.testRepository(repositoryName);
        display.setPrompt(Joiner.on('/').join(server, repositoryName));
        repository = repositoryName;
    }

    /**
     * Stop using repository which name is supplied, if applicable. Does nothing otherwise.
     *
     * @param repositoryName Repository name.
     */
    public void stopUse(String repositoryName) {
        if (repository != null && repository.equals(repositoryName)) {
            repository = null;
            if (server == null) {
                display.resetPrompt();
            } else {
                display.setPrompt(server);
            }
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
