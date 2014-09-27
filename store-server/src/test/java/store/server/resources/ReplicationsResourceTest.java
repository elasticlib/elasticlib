package store.server.resources;

import java.nio.file.Paths;
import static java.util.Collections.singletonList;
import java.util.List;
import javax.ws.rs.core.Application;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import store.common.client.Client;
import store.common.hash.Guid;
import store.common.model.ReplicationInfo;
import store.common.model.RepositoryDef;
import store.server.service.ReplicationsService;

/**
 * Unit tests.
 */
public class ReplicationsResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationsResourceTest.class);
    private final ReplicationsService replicationsService = mock(ReplicationsService.class);
    private final String source = "source";
    private final String destination = "destination";

    /**
     * Constructor.
     */
    public ReplicationsResourceTest() {
        super(LOG);
        registerMocks(replicationsService);
    }

    @Override
    protected Application configure() {
        return new ResourceConfig()
                .register(ReplicationsResource.class)
                .register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(replicationsService).to(ReplicationsService.class);
            }
        });
    }

    /**
     * Test.
     */
    @Test
    public void createReplicationTest() {
        try (Client client = newClient()) {
            client.replications().create(source, destination);
        }
        verify(replicationsService).createReplication(source, destination);
    }

    /**
     * Test.
     */
    @Test
    public void deleteReplicationTest() {
        try (Client client = newClient()) {
            client.replications().delete(source, destination);
        }
        verify(replicationsService).deleteReplication(source, destination);
    }

    /**
     * Test.
     */
    @Test
    public void startReplicationTest() {
        try (Client client = newClient()) {
            client.replications().start(source, destination);
        }
        verify(replicationsService).startReplication(source, destination);
    }

    /**
     * Test.
     */
    @Test
    public void stopReplicationTest() {
        try (Client client = newClient()) {
            client.replications().stop(source, destination);
        }
        verify(replicationsService).stopReplication(source, destination);
    }

    /**
     * Test.
     */
    @Test
    public void listReplicationsTest() {
        List<ReplicationInfo> infos = singletonList(new ReplicationInfo(def(source), def(destination)));

        when(replicationsService.listReplicationInfos()).thenReturn(infos);
        try (Client client = newClient()) {
            assertThat(client.replications().listInfos()).isEqualTo(infos);
        }
    }

    private static RepositoryDef def(String name) {
        return new RepositoryDef(name, Guid.random(), Paths.get("/tmp", name));
    }
}
