package org.elasticlib.common.config;

import java.net.URI;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.isValidDuration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import org.elasticlib.common.value.Value;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Units tests.
 */
public class ConfigUtilTest {

    private static final String KEY = "test";

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "isValidDurationDataProvider")
    public Object[][] isValidDurationDataProvider() {
        return new Object[][]{
            {"10    us", true},
            {"10 us", true},
            {"100   MicroSeconds", true},
            {"5 MS", true},
            {"5\tMILLISECONDS", true},
            {"20 s", true},
            {"30  seconds", true},
            {"30 Min", true},
            {"30  Minutes", true},
            {"1 h", true},
            {"1 hours", true},
            {"10us", false},
            {"MicroSeconds", false},
            {"5", false},
            {"ten seconds", false},
            {"5 DAYS", false},
            {"20 ko", false}
        };
    }

    /**
     * Test.
     *
     * @param value Value.
     * @param expected Expected result.
     */
    @Test(dataProvider = "isValidDurationDataProvider")
    public void isValidDurationTest(String value, boolean expected) {
        assertThat(isValidDuration(value)).as(value).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "durationDataProvider")
    public Object[][] durationDataProvider() {
        return new Object[][]{
            {"10    us", 10},
            {"10 us", 10},
            {"100   MicroSeconds", 100},
            {"5 MS", 5},
            {"5\tMILLISECONDS", 5},
            {"20 s", 20},
            {"30  seconds", 30},
            {"30 Min", 30},
            {"30  Minutes", 30},
            {"1 h", 1},
            {"1 hours", 1}
        };
    }

    /**
     * Test.
     *
     * @param value Config value.
     * @param expected Expected duration.
     */
    @Test(dataProvider = "durationDataProvider")
    public void durationTest(String value, long expected) {
        Config config = new Config().set(KEY, value);
        assertThat(duration(config, KEY)).as(value).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "unitDataProvider")
    public Object[][] unitDataProvider() {
        return new Object[][]{
            {"1 ns", TimeUnit.NANOSECONDS},
            {"10    nanoseconds", TimeUnit.NANOSECONDS},
            {"10 us", TimeUnit.MICROSECONDS},
            {"100 MicroSeconds", TimeUnit.MICROSECONDS},
            {"5  MS", TimeUnit.MILLISECONDS},
            {"5  MILLISECONDS", TimeUnit.MILLISECONDS},
            {"20  s", TimeUnit.SECONDS},
            {"30    seconds", TimeUnit.SECONDS},
            {"30 Min", TimeUnit.MINUTES},
            {"30   Minutes", TimeUnit.MINUTES},
            {"1     h", TimeUnit.HOURS},
            {"1 hours", TimeUnit.HOURS}
        };
    }

    /**
     * Test.
     *
     * @param value Config value.
     * @param expected Expected time unit.
     */
    @Test(dataProvider = "unitDataProvider")
    public void unitTest(String value, TimeUnit expected) {
        Config config = new Config().set(KEY, value);
        assertThat(unit(config, KEY)).as(value).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "malformedDurationDataProvider")
    public Object[][] malformedDurationDataProvider() {
        return new Object[][]{
            {"10us"},
            {"MicroSeconds"},
            {"5"},
            {"ten seconds"}
        };
    }

    /**
     * Test.
     *
     * @param value Config value.
     */
    @Test(dataProvider = "malformedDurationDataProvider", expectedExceptions = ConfigException.class)
    public void malformedDurationTest(String value) {
        Config config = new Config().set(KEY, value);
        duration(config, KEY);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "malformedUnitDataProvider")
    public Object[][] malformedUnitDataProvider() {
        return new Object[][]{
            {"5"},
            {"5 DAYS"},
            {"20 ko"}
        };
    }

    /**
     * Test.
     *
     * @param value Config value.
     */
    @Test(dataProvider = "malformedUnitDataProvider", expectedExceptions = ConfigException.class)
    public void malformedUnitTest(String value) {
        Config config = new Config().set(KEY, value);
        unit(config, KEY);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "urisDataProvider")
    public Object[][] urisDataProvider() {
        return new Object[][]{
            {value("http://127.0.0.1:9400"),
             uris("http://127.0.0.1:9400")},
            {value("http://192.168.1.1:9400", "http://192.168.1.2:9400"),
             uris("http://192.168.1.1:9400", "http://192.168.1.2:9400")}
        };
    }

    private static Value value(String first, String... others) {
        if (others.length == 0) {
            return Value.of(first);
        }
        List<Value> values = new ArrayList<>();
        values.add(Value.of(first));
        for (String other : others) {
            values.add(Value.of(other));
        }
        return Value.of(values);
    }

    private static List<URI> uris(String first, String... others) {
        List<URI> uris = new ArrayList<>();
        uris.add(URI.create(first));
        for (String other : others) {
            uris.add(URI.create(other));
        }
        return uris;
    }

    /**
     * Test.
     *
     * @param value Config value.
     * @param expected Expected URI(s).
     */
    @Test(dataProvider = "urisDataProvider")
    public void urisTest(Value value, List<URI> expected) {
        Config config = new Config().set(KEY, value);
        assertThat(ConfigUtil.uris(config, KEY)).as(value.toString()).isEqualTo(expected);
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "malformedUrisDataProvider")
    public Object[][] malformedUrisDataProvider() {
        return new Object[][]{
            {Value.of(true)},
            {Value.of(asList(Value.of("http://127.0.0.1:9400"), Value.of(42)))},
            {Value.of("http://127.0.0.1:9400?p=^test")}
        };
    }

    /**
     * Test.
     *
     * @param value Config value.
     */
    @Test(dataProvider = "malformedUrisDataProvider", expectedExceptions = ConfigException.class)
    public void malformedUrisTest(Value value) {
        Config config = new Config().set(KEY, value);
        ConfigUtil.uris(config, KEY);
    }
}
