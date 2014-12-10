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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.core.Response.Status;
import org.elasticlib.common.exception.BadRequestException;
import org.elasticlib.common.exception.RangeNotSatisfiableException;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class RangeTest {

    private static final long LENGTH = 1000;

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getOffsetAndLengthDataProvider")
    public Object[][] getOffsetAndLengthDataProvider() {
        return new Object[][]{
            {null, 0, LENGTH},
            {"", 0, LENGTH},
            {"bytes=0-999", 0, LENGTH},
            {"items=0-1", 0, LENGTH},
            {"bytes=0-0", 0, 1},
            {"bytes=0-99", 0, 100},
            {"bytes=100-199", 100, 100},
            {"bytes=900-999", 900, 100},
            {"bytes=900-", 900, LENGTH - 900},
            {"bytes=-100", LENGTH - 100, 100}
        };
    }

    /**
     * Test.
     *
     * @param input Range HTTP header parameter.
     * @param offset Expected offset.
     * @param length Expected length.
     */
    @Test(dataProvider = "getOffsetAndLengthDataProvider")
    public void getOffsetAndLengthTest(String input, long offset, long length) {
        Range range = new Range(input, LENGTH);

        assertThat(range.getOffset()).as("offset: " + Objects.toString(input)).isEqualTo(offset);
        assertThat(range.getLength()).as("length: " + Objects.toString(input)).isEqualTo(length);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getStatusDataProvider")
    public Object[][] getStatusDataProvider() {
        return new Object[][]{
            {null, Status.OK},
            {"", Status.OK},
            {"bytes=0-999", Status.OK},
            {"bytes=0-99", Status.PARTIAL_CONTENT}
        };
    }

    /**
     * Test.
     *
     * @param input Range HTTP header parameter.
     * @param expected Expected response status.
     */
    @Test(dataProvider = "getStatusDataProvider")
    public void getStatusTest(String input, Status expected) {
        Range range = new Range(input, LENGTH);
        assertThat(range.getStatus()).as(Objects.toString(input)).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "getHttpResponseHeadersDataProvider")
    public Object[][] getHttpResponseHeadersDataProvider() {
        return new Object[][]{
            {null, ImmutableMap.of(HttpHeaders.ACCEPT_RANGES, "bytes",
                                   HttpHeaders.CONTENT_LENGTH, "1000")},
            {"bytes=0-0", headers("1", "0-0")},
            {"bytes=0-99", headers("100", "0-99")},
            {"bytes=100-199", headers("100", "100-199")},
            {"bytes=900-999", headers("100", "900-999")},
            {"bytes=900-", headers("100", "900-999")},
            {"bytes=-100", headers("100", "900-999")}
        };
    }

    private static Map<String, String> headers(String length, String range) {
        return ImmutableMap.of(HttpHeaders.ACCEPT_RANGES, "bytes",
                               HttpHeaders.CONTENT_LENGTH, length,
                               HttpHeaders.CONTENT_RANGE, range + "/" + LENGTH);
    }

    /**
     * Test.
     *
     * @param input Range HTTP header parameter.
     * @param expected Expected response headers.
     */
    @Test(dataProvider = "getHttpResponseHeadersDataProvider")
    public void getHttpResponseHeadersTest(String input, Map<String, String> expected) {
        Map<String, String> actual = new Range(input, LENGTH).getHttpResponseHeaders();
        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "invalidRangeDataProvider")
    public Object[][] invalidRangeDataProvider() {
        return new Object[][]{
            {"bytes"},
            {"bytes=10=10"},
            {"bytes=-"},
            {"bytes=10-trash"},
            {"bytes=100-0"},
            {"bytes=0-10-100"}
        };
    }

    /**
     * Test.
     *
     * @param input Range HTTP header parameter.
     */
    @Test(dataProvider = "invalidRangeDataProvider", expectedExceptions = BadRequestException.class)
    public void invalidRangeTest(String input) {
        new Range(input, LENGTH).getStatus();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "notSatisfiableRangeDataProvider")
    public Object[][] notSatisfiableRangeDataProvider() {
        return new Object[][]{
            {"bytes=0-0,10-100"},
            {"bytes=500-1200"},
            {"bytes=1-1000"},
            {"bytes=0-1000"},
            {"bytes=0-1200"},
            {"bytes=-1200"},
            {"bytes=1200-"}
        };
    }

    /**
     * Test.
     *
     * @param input Range HTTP header parameter.
     */
    @Test(dataProvider = "notSatisfiableRangeDataProvider", expectedExceptions = RangeNotSatisfiableException.class)
    public void notSatisfiableRangeTest(String input) {
        new Range(input, LENGTH).getStatus();
    }
}
