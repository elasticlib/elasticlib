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

import java.net.URI;
import static java.time.Instant.now;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.List;
import javax.ws.rs.core.Application;
import org.elasticlib.common.client.RemotesTarget;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.node.service.RemotesService;
import static org.fest.assertions.api.Assertions.assertThat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Integration tests on the remotes resource and the HTTP client. Service layer is mocked in these tests.
 */
public class RemotesResourceTest extends AbstractResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RemotesResourceTest.class);
    private final RemotesService remotesService = mock(RemotesService.class);
    private RemotesTarget remotes;

    /**
     * Constructor.
     */
    public RemotesResourceTest() {
        super(LOG);
        registerMocks(remotesService);
    }

    @Override
    protected Application testConfiguration() {
        return new ResourceConfig()
                .register(RemotesResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(remotesService).to(RemotesService.class);
                    }
                });
    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
        remotes = clientTarget().remotes();
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
        remotes.add(uris);
        verify(remotesService).addRemote(uris);
    }

    /**
     * Test.
     */
    @Test
    public void removeRemoteTest() {
        Guid guid = Guid.random();

        remotes.remove(guid);
        verify(remotesService).removeRemote(guid.asHexadecimalString());
    }

    /**
     * Test.
     */
    @Test
    public void listRemotesTest() {
        NodeDef def = new NodeDef("test", Guid.random(), singletonList(getBaseUri()));
        NodeInfo info = new NodeInfo(def, emptyList());
        List<RemoteInfo> expected = singletonList(new RemoteInfo(info, now()));

        when(remotesService.listRemotes()).thenReturn(expected);
        assertThat(remotes.listInfos()).isEqualTo(expected);
    }
}
