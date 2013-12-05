package store.client;

import java.io.Closeable;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private final RestClient restClient;
    private String volume;
    private String index;

    public Session() {
        restClient = new RestClient();
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public void unsetVolume() {
        volume = null;
    }

    public String getVolume() {
        return volume;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void unsetIndex() {
        index = null;
    }

    public String getIndex() {
        return index;
    }

    public RestClient getRestClient() {
        return restClient;
    }

    @Override
    public void close() {
        restClient.close();
    }
}
