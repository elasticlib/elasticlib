package org.elasticlib.common.config;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.net.URI;
import java.net.URISyntaxException;
import static java.util.Collections.singletonList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;

/**
 * Configuration utilities.
 */
public final class ConfigUtil {

    private static final String NUMBER_REGEX = "^[0-9]+$";
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
     * Check whether supplied value represents a valid duration with its unit.
     *
     * @param value A string value.
     * @return True If a duration and unit can be extracted from this value.
     */
    public static boolean isValidDuration(String value) {
        List<String> parts = split(value);
        return parts.size() == 2 &&
                parts.get(0).matches(NUMBER_REGEX) &&
                UNIT_MAPPING.containsKey(parts.get(1).toLowerCase());
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
        if (parts.size() != 2 || !parts.get(0).matches(NUMBER_REGEX)) {
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

    /**
     * Extract URI(s) from config. Value associated with supplied key may be either a string or a list of strings. In
     * the first case, a single URI is parsed. In the second one, a URI is parsed from each string of the list.
     *
     * @param config Configuration holder.
     * @param key Config key.
     * @return A list of URI.
     */
    public static List<URI> uris(Config config, String key) {
        Value configVal = config.get(key);
        if (configVal.type() == ValueType.STRING) {
            return singletonList(parseUri(key, configVal));
        }
        if (configVal.type() == ValueType.ARRAY) {
            return configVal.asList()
                    .stream()
                    .map(value -> parseUri(key, value))
                    .collect(toList());
        }
        throw new ConfigException("Key " + key + " is expected to be associated with a string or a list of strings");
    }

    private static URI parseUri(String key, Value value) {
        if (value.type() != ValueType.STRING) {
            throw new ConfigException("Key " + key + ": unexpected type " + value.type().name().toLowerCase());
        }
        try {
            return new URI(value.asString());

        } catch (URISyntaxException e) {
            throw new ConfigException("Key " + key + ": ", e);
        }
    }
}
