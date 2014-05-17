package store.common.config;

import java.util.concurrent.TimeUnit;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;

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
            {"30 M", 30},
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
    public static void durationTest(String value, long expected) {
        Config config = new Config().set(KEY, value);
        assertThat(duration(config, KEY)).isEqualTo(expected);
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
    public static void unitTest(String value, TimeUnit expected) {
        Config config = new Config().set(KEY, value);
        assertThat(unit(config, KEY)).isEqualTo(expected);
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
    public static void malformedDurationTest(String value) {
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
    public static void malformedUnitTest(String value) {
        Config config = new Config().set(KEY, value);
        unit(config, KEY);
    }
}
