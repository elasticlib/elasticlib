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
package org.elasticlib.common.model;

import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.ReplicationInfo.ReplicationInfoBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Replication info unit tests.
 */
public class ReplicationInfoTest {

    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getType")
    public Object[][] getTypeDataProvider() {
        return new Object[][]{
            {local(SOURCE), local(DESTINATION), ReplicationType.LOCAL},
            {local(SOURCE), remote(DESTINATION), ReplicationType.PUSH},
            {remote(SOURCE), local(DESTINATION), ReplicationType.PULL},
            {remote(SOURCE), remote(DESTINATION), ReplicationType.REMOTE},
            {local(SOURCE), null, ReplicationType.PUSH},
            {null, local(DESTINATION), ReplicationType.PULL},
            {remote(SOURCE), null, ReplicationType.REMOTE},
            {null, remote(DESTINATION), ReplicationType.REMOTE},
            {null, null, ReplicationType.REMOTE}
        };
    }

    /**
     * Test.
     *
     * @param source Replication source repository definition.
     * @param destination Replication destination repository definition.
     * @param expected Expected replication type.
     */
    @Test(dataProvider = "getType")
    public void getTypeTest(RepositoryDef source, RepositoryDef destination, ReplicationType expected) {
        ReplicationType actual = new ReplicationInfoBuilder(Guid.random(), guid(source), guid(destination))
                .withSourceDef(source)
                .withDestinationDef(destination)
                .build()
                .getType();

        assertThat(actual).isEqualTo(expected);
    }

    private static RepositoryDef local(String name) {
        return new RepositoryDef(name, Guid.random(), "/tmp/" + name);
    }

    private static RepositoryDef remote(String name) {
        return new RepositoryDef("remote." + name, Guid.random(), "http://127.0.0.1:9400/repositories/" + name);
    }

    private static Guid guid(RepositoryDef nullable) {
        if (nullable == null) {
            return Guid.random();
        }
        return nullable.getGuid();
    }
}
