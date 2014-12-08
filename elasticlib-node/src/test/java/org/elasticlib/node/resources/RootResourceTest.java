package org.elasticlib.node.resources;

import static java.util.Collections.singletonList;
import javax.ws.rs.core.Application;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.exception.UnexpectedFailureException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.node.service.NodesService;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Integration tests on the root resource and the HTTP client. Service layer is mocked in these tests.
 */
public class RootResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RootResourceTest.class);
    private final NodesService nodesService = mock(NodesService.class);

    /**
     * Constructor.
     */
    public RootResourceTest() {
        super(LOG);
        registerMocks(nodesService);
    }

    @Override
    protected Application testConfiguration() {
        return new ResourceConfig()
                .register(RootResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(nodesService).to(NodesService.class);
                    }
                });
    }

    /**
     * Test.
     */
    @Test
    public void getNodeDefTest() {
        NodeDef nodeDef = new NodeDef("test", Guid.random(), singletonList(getBaseUri()));
        when(nodesService.getNodeDef()).thenReturn(nodeDef);
        try (Client client = newClient()) {
            assertThat(client.node().getDef()).isEqualTo(nodeDef);
        }
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnexpectedFailureException.class)
    public void getNodeDefWithUnexpectedFailureTest() {
        when(nodesService.getNodeDef()).thenThrow(new NullPointerException());
        try (Client client = newClient()) {
            client.node().getDef();
        }
    }
}
