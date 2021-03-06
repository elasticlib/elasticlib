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
import org.elasticlib.common.client.ClientTarget;
import static org.elasticlib.node.TestUtil.config;
import org.elasticlib.node.manager.client.ClientManager;
import org.elasticlib.node.providers.HttpExceptionMapper;
import org.elasticlib.node.providers.MappableBodyWriter;
import org.elasticlib.node.providers.MappableListBodyWriter;
import org.elasticlib.node.providers.MultipartReader;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg.ContainerPerClassTest;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyTestContainerFactory;
import org.mockito.Mockito;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;

/**
 * Parent class for REST resources tests.
 */
public abstract class AbstractResourceTest extends ContainerPerClassTest {

    private final List<Object> mocks = new ArrayList<>();
    private final ClientManager clientManager = new ClientManager(config());

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    /**
     * Constructor.
     */
    public AbstractResourceTest() {
        super(new GrizzlyTestContainerFactory());

        // Specifies the test container to dynamically bind to an available port
        // instead of using the fixed default one.
        set(TestProperties.CONTAINER_PORT, 0);
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

    @AfterClass
    @Override
    public final void tearDown() throws Exception {
        clientManager.stop();
        super.tearDown();
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
     * Allows actual test implementation to supply specific configuration, typically tested resource class and mocked
     * services bindings. Returned value is expected to be non null.
     *
     * @return Actual test specific configuration.
     */
    protected abstract Application testConfiguration();

    /**
     * Provides a pre-configured base client API.
     *
     * @return A new base API instance.
     */
    protected ClientTarget clientTarget() {
        return clientManager.getClient().target(getBaseUri());
    }
}
