package store.server.resources;

import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import org.glassfish.jersey.test.JerseyTestNg.ContainerPerClassTest;
import org.glassfish.jersey.test.grizzly.GrizzlyTestContainerFactory;
import static org.mockito.Mockito.reset;
import org.slf4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testng.annotations.BeforeMethod;
import store.common.client.Client;
import store.server.service.ClientLoggingHandler;

/**
 * Parent class for REST resources tests.
 */
public abstract class AbstractResourceTest extends ContainerPerClassTest {

    private final Logger logger;
    private final List<Object> mocks = new ArrayList<>();

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    /**
     * Constructor.
     *
     * @param logger Logger for HTTP dialogs.
     */
    public AbstractResourceTest(Logger logger) {
        super(new GrizzlyTestContainerFactory());
        this.logger = logger;
    }

    /**
     * Resets mocks.
     */
    @BeforeMethod
    public void resetMocks() {
        for (Object mock : mocks) {
            reset(mock);
        }
    }

    /**
     * Registers mocks be to reset before each test method.
     *
     * @param instances Mocks instances.
     */
    protected void registerMocks(Object... instances) {
        mocks.addAll(asList(instances));
    }

    /**
     * Provides a pre-configured client.
     *
     * @return A new node client instance.
     */
    protected Client newClient() {
        return new Client(getBaseUri(), new ClientLoggingHandler(logger));
    }
}
