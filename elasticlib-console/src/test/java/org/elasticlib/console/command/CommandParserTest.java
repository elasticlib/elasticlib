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
package org.elasticlib.console.command;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.discovery.DiscoveryClient;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class CommandParserTest {

    private CommandParser parser;

    /**
     * Initialization.
     */
    @BeforeClass
    public void init() {
        Display display = mock(Display.class);
        Session session = mock(Session.class, RETURNS_DEEP_STUBS);
        ConsoleConfig config = mock(ConsoleConfig.class);
        DiscoveryClient discoveryClient = mock(DiscoveryClient.class);

        when(session.getClient().repositories().listInfos())
                .thenReturn(asList(new RepositoryInfo(new RepositoryDef("primary",
                                                                        new Guid("8d5f3c77e94a0cad3a32340d342135f4"),
                                                                        Paths.get("/repo/primary"))),
                                   new RepositoryInfo(new RepositoryDef("secondary",
                                                                        new Guid("0d99dd9895a2a1c485e0c75f79f92cc1"),
                                                                        Paths.get("/repo/secondary")))));

        parser = new CommandParser(display, session, config, discoveryClient);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][]{
            {"dr", asList("drop"), 0},
            {"drop", asList("drop"), 0},
            {"drop ", asList("replication", "repository"), 5},
            {"drop repo", asList("repository"), 5},
            {"drop repository", asList("repository"), 5},
            {"drop repository ", asList("0d99dd9895a2a1c485e0c75f79f92cc1", "8d5f3c77e94a0cad3a32340d342135f4",
                                        "primary", "secondary"), 16},
            {"drop repository p", asList("primary"), 16},
            {"drop repository primary", asList("primary"), 16},
            {"drop repository primary ", asList(), 24},
            {"create repli", asList("replication"), 7},
            {"ad", asList("add"), 0},
            {"add ", asList("remote", "repository"), 4},
            {"add re", asList("remote", "repository"), 4},
            {"add rem", asList("remote"), 4}
        };
    }

    /**
     * Test.
     *
     * @param buffer Buffer to complete.
     * @param expectedCandidates Expected completion candidates.
     * @param expectedIndex Expected completion index.
     */
    @Test(dataProvider = "dataProvider")
    public void completeTest(String buffer, List<CharSequence> expectedCandidates, int expectedIndex) {
        List<CharSequence> actualCandidates = new ArrayList<>();
        int actualIndex = parser.complete(buffer, buffer.length(), actualCandidates);

        assertThat(actualCandidates).as(buffer).isEqualTo(expectedCandidates);
        assertThat(actualIndex).as(buffer).isEqualTo(expectedIndex);
    }
}
