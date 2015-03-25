/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.resources;

import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import javax.ws.rs.core.Application;
import org.elasticlib.common.client.Client;
import org.elasticlib.node.components.ClientLoggingHandler;
import org.elasticlib.node.providers.HttpExceptionMapper;
import org.elasticlib.node.providers.MappableBodyWriter;
import org.elasticlib.node.providers.MappableListBodyWriter;
import org.elasticlib.node.providers.MultipartReader;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg.ContainerPerClassTest;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyTestContainerFactory;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testng.annotations.BeforeMethod;

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

        // Specifies the test container to dynamically bind to an available port
        // instead of using the fixed default one.
        set(TestProperties.CONTAINER_PORT, 0);
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
