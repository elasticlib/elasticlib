package store.server.resources;

import java.net.URI;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import java.util.List;
import javax.ws.rs.core.Application;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.joda.time.Instant;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import store.common.client.Client;
import store.common.hash.Guid;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.server.service.NodesService;

/**
 * Integration tests on the remotes resource and the HTTP client. Service layer is mocked in these tests.
 */
public class RemotesResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RemotesResourceTest.class);
    private final NodesService nodesService = mock(NodesService.class);

    /**
     * Constructor.
     */
    public RemotesResourceTest() {
        super(LOG);
        registerMocks(nodesService);
    }

    @Override
    protected Application testConfiguration() {
        return new ResourceConfig()
                .register(RemotesResource.class)
                .register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(nodesService).to(NodesService.class);
            }
        });
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "addRemoteDataProvider")
    public Object[][] addRemoteDataProvider() {
        return new Object[][]{
            {uris("http://localhost:9400")},
            {uris("http://localhost:9400", "http://192.168.0.1:9400")}
        };
    }

    private static List<URI> uris(String... values) {
        List<URI> uris = new ArrayList<>();
        for (String value : values) {
            uris.add(URI.create(value));
        }
        return uris;
    }

    /**
     * Test.
     *
     * @param uris Remote URI(s)
     */
    @Test(dataProvider = "addRemoteDataProvider")
    public void addRemoteTest(List<URI> uris) {
        try (Client client = newClient()) {
            client.remotes().add(uris);
        }
        verify(nodesService).addRemote(uris);
    }

    /**
     * Test.
     */
    @Test
    public void removeRemoteTest() {
        Guid guid = Guid.random();
        try (Client client = newClient()) {
            client.remotes().remove(guid);
        }
        verify(nodesService).removeRemote(guid.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void listRemotesTest() {
        NodeDef def = new NodeDef("test", Guid.random(), singletonList(getBaseUri()));
        List<NodeInfo> nodeInfos = singletonList(new NodeInfo(def, Instant.now()));

        when(nodesService.listRemotes()).thenReturn(nodeInfos);
        try (Client client = newClient()) {
            assertThat(client.remotes().listInfos()).isEqualTo(nodeInfos);
        }
    }
}
