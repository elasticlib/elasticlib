package store.server.resources;

import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import javax.ws.rs.core.Application;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg.ContainerPerClassTest;
import org.glassfish.jersey.test.grizzly.GrizzlyTestContainerFactory;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testng.annotations.BeforeMethod;
import store.common.client.Client;
import store.server.providers.HttpExceptionMapper;
import store.server.providers.MappableBodyWriter;
import store.server.providers.MappableListBodyWriter;
import store.server.providers.MultipartReader;
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

    @Override
    protected final Application configure() {
        Application testConfig = testConfiguration();
        return new ResourceConfig()
                .register(HttpExceptionMapper.class)
                .register(MappableBodyWriter.class)
                .register(MappableListBodyWriter.class)
                .register(MultipartReader.class)
                .registerClasses(testConfig.getClasses())
                .registerInstances(testConfig.getSingletons())
                .addProperties(testConfig.getProperties());
    }

    /**
     * Resets mocks.
     */
    @BeforeMethod
    public final void resetMocks() {
        mocks.forEach(Mockito::reset);
    }

    /**
     * Allows actual test implemetation to register mocks be to reset before each test method.
     *
     * @param instances Mocks instances.
     */
    protected void registerMocks(Object... instances) {
        mocks.addAll(asList(instances));
    }

    /**
     * Allows actual test implemetation to supply specific configuration, typically tested resource class and mocked
     * services bindings. Returned value is expected to be non null.
     *
     * @return Actual test specific configuration.
     */
    protected abstract Application testConfiguration();

    /**
     * Provides a pre-configured client.
     *
     * @return A new node client instance.
     */
    protected Client newClient() {
        return new Client(getBaseUri(), new ClientLoggingHandler(logger));
    }
}
