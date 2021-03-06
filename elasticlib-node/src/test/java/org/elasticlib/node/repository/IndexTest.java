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
package org.elasticlib.node.repository;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.elasticlib.common.model.IndexEntry;
import static org.elasticlib.node.TestUtil.LOREM_IPSUM;
import static org.elasticlib.node.TestUtil.UNKNOWN_HASH;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Index integration tests.
 */
@Test(singleThreaded = true)
public class IndexTest {

    private Path path;
    private Index index;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test
    public void create() {
        index = Index.create("test-index", path);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "create")
    public void findUnknown() {
        assertThat(index.find("UNKNOWN", 0, 20)).isEmpty();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "create")
    public void deleteUnknown() {
        index.delete(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = "emptyRead", dependsOnMethods = "create")
    public void findOnEmptyIndex() {
        assertThat(index.find("lorem", 0, 20)).isEmpty();
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnGroups = "emptyRead")
    public void index() throws IOException {
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            index.index(LOREM_IPSUM.getTree(), inputStream);
        }
    }

    /**
     * Test.
     */
    @Test(groups = "read", dependsOnMethods = "index")
    public void find() {
        IndexEntry expected = new IndexEntry(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
        assertThat(index.find("lorem", 0, 20)).containsExactly(expected);
    }

    /**
     * Test.
     */
    @Test(dependsOnGroups = "read")
    public void delete() {
        index.delete(LOREM_IPSUM.getHash());
        assertThat(index.find("lorem", 0, 20)).isEmpty();
    }
}
