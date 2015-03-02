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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import javax.ws.rs.core.Application;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.exception.UnexpectedFailureException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.node.service.NodeService;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Integration tests on the node resource and the HTTP client. Service layer is mocked in these tests.
 */
public class NodeResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(NodeResourceTest.class);
    private final NodeService nodeService = mock(NodeService.class);

    /**
     * Constructor.
     */
    public NodeResourceTest() {
        super(LOG);
        registerMocks(nodeService);
    }

    @Override
    protected Application testConfiguration() {
        return new ResourceConfig()
                .register(NodeResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(nodeService).to(NodeService.class);
                    }
                });
    }

    /**
     * Test.
     */
    @Test
    public void getNodeInfoTest() {
        NodeDef def = new NodeDef("test", Guid.random(), singletonList(getBaseUri()));
        NodeInfo info = new NodeInfo(def, emptyList());

        when(nodeService.getNodeInfo()).thenReturn(info);
        try (Client client = newClient()) {
            assertThat(client.node().getInfo()).isEqualTo(info);
        }
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnexpectedFailureException.class)
    public void getNodeInfoWithUnexpectedFailureTest() {
        when(nodeService.getNodeInfo()).thenThrow(new NullPointerException());
        try (Client client = newClient()) {
            client.node().getInfo();
        }
    }
}