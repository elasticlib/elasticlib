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
package org.elasticlib.console.display;

import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class TreePrinterTest {

    /**
     * Test.
     */
    @Test
    public void printSimpleTest() {
        String expected = text("*  a2",
                               "*  a1",
                               "*  a0");

        String actual = tree(revision("a2", "a1"),
                             revision("a1", "a0"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printMergingTest() {
        String expected = text("*    a2",
                               "|\\   ",
                               "| *  a1",
                               "*  a0");

        String actual = tree(revision("a2", "a1", "a0"),
                             revision("a1"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printTripleMergingTest() {
        String expected = text("*-.    a3",
                               "|\\ \\   ",
                               "| | *  a2",
                               "| *  a1",
                               "*  a0");

        String actual = tree(revision("a3", "a2", "a1", "a0"),
                             revision("a2"),
                             revision("a1"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printBranchingTest() {
        String expected = text("*  a2",
                               "| *  a1",
                               "|/",
                               "*  a0");

        String actual = tree(revision("a2", "a0"),
                             revision("a1", "a0"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void printTripleBranchingTest() {
        String expected = text("*  a3",
                               "| *  a2",
                               "|/",
                               "| *  a1",
                               "|/",
                               "*  a0");

        String actual = tree(revision("a3", "a0"),
                             revision("a2", "a0"),
                             revision("a1", "a0"),
                             revision("a0"));

        assertThat(actual).isEqualTo(expected);
    }

    private static String text(String... lines) {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append(System.lineSeparator());
        }
        return builder.toString();
    }

    private static String tree(Revision... revisions) {
        TreePrinter printer = new TreePrinter(2, rev -> rev.getRevision().asHexadecimalString());
        for (Revision info : revisions) {
            printer.add(info);
        }
        return printer.print();
    }

    private static Revision revision(String rev, String... parents) {
        RevisionBuilder builder = new RevisionBuilder()
                .withContent(new Hash("ff"))
                .withLength(1024);

        for (String parent : parents) {
            builder.withParent(new Hash(parent));
        }

        return builder.build(new Hash(rev));
    }
}
