package store.client.http;

import java.io.Closeable;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private final RestClient restClient;
    private String repository;

    public Session() {
        restClient = new RestClient();
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }

    public void unsetRepository() {
        repository = null;
    }

    public String getRepository() {
        return repository;
    }

    public RestClient getRestClient() {
        return restClient;
    }

    @Override
    public void close() {
        restClient.close();
    }
}
