package store.common.config;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Configuration utilities.
 */
public final class ConfigUtil {

    private static final Map<String, TimeUnit> UNIT_MAPPING = new HashMap<>();

    static {
        UNIT_MAPPING.put("ns", TimeUnit.NANOSECONDS);
        UNIT_MAPPING.put("us", TimeUnit.MICROSECONDS);
        UNIT_MAPPING.put("ms", TimeUnit.MILLISECONDS);
        UNIT_MAPPING.put("s", TimeUnit.SECONDS);
        UNIT_MAPPING.put("min", TimeUnit.MINUTES);
        UNIT_MAPPING.put("h", TimeUnit.HOURS);

        mapByName(TimeUnit.NANOSECONDS,
                  TimeUnit.MICROSECONDS,
                  TimeUnit.MILLISECONDS,
                  TimeUnit.SECONDS,
                  TimeUnit.MINUTES,
                  TimeUnit.HOURS);
    }

    private ConfigUtil() {
    }

    private static void mapByName(TimeUnit... units) {
        for (TimeUnit unit : units) {
            UNIT_MAPPING.put(unit.name().toLowerCase(), unit);
        }
    }

    /**
     * Extracts duration value from string config value.
     *
     * <tr><td>Config value</td><td>duration</td></tr>
     * <tr><td>"10 s"</td><td>10</td></tr>
     * <tr><td>"5 minutes"</td><td>5</td></tr>
     *
     * @param config Configuration holder.
     * @param key Config key.
     * @return duration value.
     */
    public static long duration(Config config, String key) {
        List<String> parts = split(config.getString(key));
        if (parts.size() != 2 || !parts.get(0).matches("^[0-9]+$")) {
            throw newMalformedDurationException(key);
        }
        return Long.parseLong(parts.get(0));
    }

    /**
     * Extracts duration unit from string config value.
     *
     * <tr><td>Config value</td><td>Unit</td></tr>
     * <tr><td>"10 s"</td><td>TimeUnit.SECONDS</td></tr>
     * <tr><td>"5 minutes"</td><td>TimeUnit.MINUTES</td></tr>
     *
     * @param config Configuration holder.
     * @param key Config key.
     * @return duration unit.
     */
    public static TimeUnit unit(Config config, String key) {
        List<String> parts = split(config.getString(key));
        if (parts.size() != 2 || !UNIT_MAPPING.containsKey(parts.get(1).toLowerCase())) {
            throw newMalformedDurationException(key);
        }
        return UNIT_MAPPING.get(parts.get(1).toLowerCase());
    }

    private static List<String> split(String duration) {
        return Splitter
                .on(CharMatcher.WHITESPACE)
                .omitEmptyStrings()
                .splitToList(duration);
    }

    private static ConfigException newMalformedDurationException(String key) {
        return new ConfigException("Key " + key + " is expected to represent a well-formed duration");
    }
}
