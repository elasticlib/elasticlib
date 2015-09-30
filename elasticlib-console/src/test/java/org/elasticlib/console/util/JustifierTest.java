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
package org.elasticlib.console.util;

import static java.lang.System.lineSeparator;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class JustifierTest {

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "justify")
    public Object[][] justifyDataProvider() {
        return new Object[][]{
            new Object[]{
                10, 0, 0,
                "",
                ""
            },
            new Object[]{
                10, 1, 0,
                "",
                " "
            },
            new Object[]{
                60, 0, 0,
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
            },
            new Object[]{
                40, 0, 0,
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                text("Lorem  ipsum dolor sit amet, consectetur",
                     "adipiscing elit.")
            },
            new Object[]{
                40, 2, 1,
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                text("  Lorem    ipsum    dolor   sit   amet, ",
                     "  consectetur adipiscing elit. ")
            },
            new Object[]{
                40, 2, 2,
                text("Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                     "",
                     "Aliquam euismod pulvinar lorem eget pulvinar."),
                text("  Lorem    ipsum   dolor   sit   amet,  ",
                     "  consectetur adipiscing elit.  ",
                     "    ",
                     "  Aliquam  euismod pulvinar lorem eget  ",
                     "  pulvinar.  ")
            },
            new Object[]{
                45, 0, 0,
                "* Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                text("* Lorem  ipsum  dolor  sit  amet, consectetur",
                     "adipiscing elit.")
            },
            new Object[]{
                45, 0, 0,
                "Lorem ipsum * dolor sit amet, consectetur adipiscing elit.",
                text("Lorem  ipsum * dolor  sit  amet,  consectetur",
                     "adipiscing elit.")
            },
            new Object[]{
                45, 0, 0,
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. *",
                text("Lorem   ipsum  dolor  sit  amet,  consectetur",
                     "adipiscing elit. *")
            }
        };
    }

    /**
     * Test.
     *
     * @param width Ouput width.
     * @param paddingLeft Output left padding.
     * @param paddingRight Output right padding.
     * @param text Test input.
     * @param expected Expected output.
     */
    @Test(dataProvider = "justify")
    public void justifyTest(int width, int paddingLeft, int paddingRight, String text, String expected) {
        String actual = Justifier.width(width)
                .paddingLeft(paddingLeft)
                .paddingRight(paddingRight)
                .fixed("*")
                .justify(text);

        assertThat(actual).isEqualTo(expected);
    }

    private static String text(String... lines) {
        return String.join(lineSeparator(), lines);
    }
}
