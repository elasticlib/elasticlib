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
package org.elasticlib.node.components;

import java.net.URI;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import java.util.List;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.RepositoryStats;
import org.elasticlib.node.manager.message.NewRepositoryEvent;
import org.elasticlib.node.manager.message.RepositoryAvailable;
import org.elasticlib.node.manager.message.RepositoryChangeMessage;
import org.elasticlib.node.manager.message.RepositoryRemoved;
import org.elasticlib.node.manager.message.RepositoryUnavailable;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Remote nodes messages factory unit tests.
 */
public class RemoteNodesMessagesFactoryTest {

    private static final String NAME = "test";
    private static final String PATH = "/tmp/test";
    private static final Guid GUID = new Guid("eac7690f2ca05940e9239d5300037551");
    private static final long SEQ = 1;
    private static final URI NODE_URI = URI.create("http://localhost:9400");

    private final RemoteNodesMessagesFactory factory = new RemoteNodesMessagesFactory();

    /**
     * Creation messages data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "create")
    public Object[][] createDataProvider() {
        return new Object[][]{
            test(unreachable(closed(GUID))),
            test(unreachable(open(GUID))),
            test(reachable(closed(GUID))),
            test(reachable(open(GUID)), new RepositoryAvailable(GUID))
        };
    }

    /**
     * Test.
     *
     * @param info Created remote info.
     * @param expected Expected messages.
     */
    @Test(dataProvider = "create")
    public void createMessagesTest(RemoteInfo info, List<RepositoryChangeMessage> expected) {
        assertThat(factory.createMessages(info)).isEqualTo(expected);
    }

    /**
     * Deletion messages data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "delete")
    public Object[][] deleteDataProvider() {
        return new Object[][]{
            test(unreachable(closed(GUID))),
            test(unreachable(open(GUID))),
            test(reachable(closed(GUID))),
            test(reachable(open(GUID)), new RepositoryUnavailable(GUID))
        };
    }

    /**
     * Test.
     *
     * @param info Deleted remote info.
     * @param expected Expected messages.
     */
    @Test(dataProvider = "delete")
    public void deleteMessagesTest(RemoteInfo info, List<RepositoryChangeMessage> expected) {
        assertThat(factory.deleteMessages(info)).isEqualTo(expected);
    }

    /**
     * Update messages data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "update")
    public Object[][] updateDataProvider() {
        return new Object[][]{
            test(unreachable(open(GUID)), unreachable(open(GUID))),
            test(unreachable(open(GUID)), reachable(open(GUID)), new RepositoryAvailable(GUID)),
            test(unreachable(open(GUID)), reachable(closed(GUID))),
            test(reachable(open(GUID)), unreachable(open(GUID)), new RepositoryUnavailable(GUID)),
            test(reachable(closed(GUID)), unreachable(closed(GUID))),
            test(reachable(closed(GUID)), reachable(closed(GUID))),
            test(reachable(closed(GUID)), reachable(), new RepositoryRemoved(GUID)),
            test(reachable(closed(GUID)), reachable(open(GUID)), new RepositoryAvailable(GUID)),
            test(reachable(open(GUID)), reachable(closed(GUID)), new RepositoryUnavailable(GUID)),
            test(reachable(open(GUID)), reachable(open(GUID))),
            test(reachable(open(GUID, SEQ)), reachable(open(GUID, SEQ + 1)), new NewRepositoryEvent(GUID))
        };
    }

    /**
     * Test.
     *
     * @param before Info before the update.
     * @param after Info after the update.
     * @param expected Expected messages.
     */
    @Test(dataProvider = "update")
    public void updateMessagesTest(RemoteInfo before, RemoteInfo after, List<RepositoryChangeMessage> expected) {
        assertThat(factory.updateMessages(before, after)).isEqualTo(expected);
    }

    private static Object[] test(RemoteInfo info, RepositoryChangeMessage... expected) {
        return new Object[]{info, asList(expected)};
    }

    private static Object[] test(RemoteInfo before, RemoteInfo after, RepositoryChangeMessage... expected) {
        return new Object[]{before, after, asList(expected)};
    }

    private static RemoteInfo unreachable(RepositoryInfo... repositoryInfo) {
        return new RemoteInfo(nodeInfo(repositoryInfo), now());
    }

    private static RemoteInfo reachable(RepositoryInfo... repositoryInfo) {
        return new RemoteInfo(nodeInfo(repositoryInfo), NODE_URI, now());
    }

    private static NodeInfo nodeInfo(RepositoryInfo... repositoryInfo) {
        return new NodeInfo(new NodeDef(NAME, GUID, singletonList(NODE_URI)),
                            asList(repositoryInfo));
    }

    private static RepositoryInfo open(Guid guid, long seq) {
        return new RepositoryInfo(new RepositoryDef(NAME, guid, PATH),
                                  new RepositoryStats(seq, 0, 0, emptyMap()),
                                  new AgentInfo(seq, seq, AgentState.WAITING),
                                  new AgentInfo(seq, seq, AgentState.WAITING));
    }

    private static RepositoryInfo open(Guid guid) {
        return open(guid, SEQ);
    }

    private static RepositoryInfo closed(Guid guid) {
        return new RepositoryInfo(new RepositoryDef(NAME, guid, PATH));
    }
}
