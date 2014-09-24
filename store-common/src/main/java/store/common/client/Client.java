package store.common.client;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * A node HTTP client.
 */
public class Client implements AutoCloseable {

    private final javax.ws.rs.client.Client client;
    private final NodeClient nodeClient;
    private final RemotesClient remotesClient;
    private final RepositoriesClient repositoriesClient;
    private final ReplicationsClient replicationsClient;

    /**
     * Constructor.
     *
     * @param uri Node base URI.
     * @param loggingHandler Logging handler.
     */
    public Client(URI uri, LoggingHandler loggingHandler) {
        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        ClientConfig clientConfig = new ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024)
                .connectorProvider(new ApacheConnectorProvider())
                .register(MultiPartFeature.class)
                .register(new HeaderRestoringWriterInterceptor())
                .register(new LoggingFilter(logger, true))
                .register(new ClientLoggingFilter(loggingHandler));

        client = ClientBuilder.newClient(clientConfig);

        WebTarget resource = client.target(uri);
        nodeClient = new NodeClient(resource);
        remotesClient = new RemotesClient(resource);
        repositoriesClient = new RepositoriesClient(resource);
        replicationsClient = new ReplicationsClient(resource);
    }

    /**
     * @return The node API client.
     */
    public NodeClient node() {
        return nodeClient;
    }

    /**
     * @return The remotes API client.
     */
    public RemotesClient remotes() {
        return remotesClient;
    }

    /**
     * @return The repositories API client.
     */
    public RepositoriesClient repositories() {
        return repositoriesClient;
    }

    /**
     * @return The replications API client.
     */
    public ReplicationsClient replications() {
        return replicationsClient;
    }

    @Override
    public void close() {
        client.close();
    }
}
