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

import static java.util.Collections.singletonList;
import java.util.List;
import javax.ws.rs.core.Application;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.ReplicationInfo.ReplicationInfoBuilder;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.node.service.ReplicationsService;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Integration tests on the replications resource and the HTTP client. Service layer is mocked in these tests.
 */
public class ReplicationsResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationsResourceTest.class);
    private final ReplicationsService replicationsService = mock(ReplicationsService.class);
    private final Guid guid = Guid.random();
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
    protected Application testConfiguration() {
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
            client.replications().delete(guid);
        }
        verify(replicationsService).deleteReplication(guid);
    }

    /**
     * Test.
     */
    @Test
    public void startReplicationTest() {
        try (Client client = newClient()) {
            client.replications().start(guid);
        }
        verify(replicationsService).startReplication(guid);
    }

    /**
     * Test.
     */
    @Test
    public void stopReplicationTest() {
        try (Client client = newClient()) {
            client.replications().stop(guid);
        }
        verify(replicationsService).stopReplication(guid);
    }

    /**
     * Test.
     */
    @Test
    public void listReplicationsTest() {
        List<ReplicationInfo> infos = singletonList(newReplicationInfo(source, destination));

        when(replicationsService.listReplicationInfos()).thenReturn(infos);
        try (Client client = newClient()) {
            assertThat(client.replications().listInfos()).isEqualTo(infos);
        }
    }

    private static ReplicationInfo newReplicationInfo(String source, String destination) {
        RepositoryDef sourceDef = newRepositoryDef(source);
        RepositoryDef destinationDef = newRepositoryDef(destination);
        return new ReplicationInfoBuilder(Guid.random(), sourceDef.getGuid(), destinationDef.getGuid())
                .withSourceDef(sourceDef)
                .withDestinationDef(destinationDef)
                .build();
    }

    private static RepositoryDef newRepositoryDef(String name) {
        return new RepositoryDef(name, Guid.random(), "/tmp/" + name);
    }
}
