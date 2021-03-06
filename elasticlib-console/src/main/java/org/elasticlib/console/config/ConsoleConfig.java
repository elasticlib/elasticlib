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
package org.elasticlib.console.config;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.config.ConfigException;
import org.elasticlib.common.config.ConfigReadWrite;
import static org.elasticlib.common.config.ConfigReadWrite.readFromClassPath;
import org.elasticlib.common.config.ConfigUtil;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import org.elasticlib.console.exception.RequestFailedException;
import static org.elasticlib.console.util.ClientUtil.parseUri;
import static org.elasticlib.console.util.Directories.home;

/**
 * Console config.
 */
public class ConsoleConfig {

    private static final String DEFAULT_NODE = "default.node";
    private static final String DEFAULT_REPOSITORY = "default.repository";
    private static final String DISCOVERY_ENABLED = "discovery.enabled";
    private static final String DISCOVERY_GROUP = "discovery.group";
    private static final String DISCOVERY_PORT = "discovery.port";
    private static final String DISCOVERY_TTL = "discovery.ttl";
    private static final String DISCOVERY_PING_INTERVAL = "discovery.pingInterval";
    private static final String DISPLAY_FORMAT = "display.format";
    private static final String DISPLAY_COLOR = "display.color";
    private static final String DISPLAY_PRETTY = "display.pretty";
    private static final String DISPLAY_PROGRESS = "display.progress";
    private static final String DISPLAY_HTTP = "display.http";
    private static final String EDITOR = "editor";
    private static final Path CONFIG_PATH = home().resolve("config.yml");
    private static final Config DEFAULT = readFromClassPath(ConsoleConfig.class, "config.yml");

    private Config extended;
    private Config config;

    /**
     * Initialisation. Actually load config.
     */
    public void init() {
        try {
            Config loaded = ConfigReadWrite.read(CONFIG_PATH);
            if (!loaded.isEmpty()) {
                List<String> validKeys = listKeys();
                for (String key : loaded.listKeys()) {
                    if (!validKeys.contains(key)) {
                        throw new ConfigException(undefinedConfigKey(key));
                    }
                }
                config = loaded;

            } else {
                config = Config.empty();
            }
            extended = DEFAULT.extend(config);

        } catch (ConfigException e) {
            config = Config.empty();
            extended = DEFAULT.extend(config);
            throw new RequestFailedException("Could not load config, using default. Cause:", e);
        }
    }

    /**
     * @return All valid config keys.
     */
    public static List<String> listKeys() {
        return DEFAULT.listKeys();
    }

    /**
     * @return A printable view of this config.
     */
    public String print() {
        return extended.toString();
    }

    /**
     * @return The URI of the default node to connect to. May be null.
     */
    public URI getDefaultNode() {
        String value = extended.getString(DEFAULT_NODE);
        if (value.isEmpty()) {
            return null;
        }
        return parseUri(value);
    }

    /**
     * @return Default repository to use. May be empty.
     */
    public String getDefaultRepository() {
        return extended.getString(DEFAULT_REPOSITORY);
    }

    /**
     * @return Whether multicast discovery is enabled.
     */
    public boolean isDiscoveryEnabled() {
        return extended.getBoolean(DISCOVERY_ENABLED);
    }

    /**
     * @return Multicast discovery group address.
     */
    public String getDiscoveryGroup() {
        return extended.getString(DISCOVERY_GROUP);
    }

    /**
     * @return Multicast discovery port.
     */
    public int getDiscoveryPort() {
        return extended.getInt(DISCOVERY_PORT);
    }

    /**
     * @return Multicast discovery packets time to live.
     */
    public int getDiscoveryTimeToLive() {
        return extended.getInt(DISCOVERY_TTL);
    }

    /**
     * @return Multicast discovery ping interval duration.
     */
    public long getDiscoveryPingIntervalDuration() {
        return duration(extended, DISCOVERY_PING_INTERVAL);
    }

    /**
     * @return Multicast discovery ping interval time unit.
     */
    public TimeUnit getDiscoveryPingIntervalUnit() {
        return unit(extended, DISCOVERY_PING_INTERVAL);
    }

    /**
     * @return Used format for entities rendering.
     */
    public Format getDisplayFormat() {
        return Format.fromString(extended.getString(DISPLAY_FORMAT));
    }

    /**
     * @return If console output should be colored.
     */
    public boolean isDisplayColor() {
        return extended.getBoolean(DISPLAY_COLOR);
    }

    /**
     * @return If displayed values should be nicely formatted.
     */
    public boolean isDisplayPretty() {
        return extended.getBoolean(DISPLAY_PRETTY);
    }

    /**
     * @return If progress status for long operations should be displayed.
     */
    public boolean isDisplayProgress() {
        return extended.getBoolean(DISPLAY_PROGRESS);
    }

    /**
     * @return If HTTP dialog should be displayed.
     */
    public boolean isDisplayHttp() {
        return extended.getBoolean(DISPLAY_HTTP);
    }

    /**
     * @return External editor to use.
     */
    public String getEditor() {
        return extended.getString(EDITOR);
    }

    /**
     * Set a config key.
     *
     * @param key Key.
     * @param value Value.
     */
    public void set(String key, String value) {
        switch (key) {
            case DEFAULT_NODE:
                config = config.set(key, parseUri(value).toString());
                break;

            case DEFAULT_REPOSITORY:
            case EDITOR:
                config = config.set(key, value);
                break;

            case DISCOVERY_GROUP:
                config = config.set(key, checkGroup(value));
                break;

            case DISCOVERY_PORT:
                config = config.set(key, checkInterval(value, 1, 65535));
                break;

            case DISCOVERY_TTL:
                config = config.set(key, checkInterval(value, 0, 255));
                break;

            case DISCOVERY_PING_INTERVAL:
                config = config.set(key, checkDuration(value));
                break;

            case DISPLAY_FORMAT:
                config = config.set(key, checkFormat(value));
                break;

            case DISCOVERY_ENABLED:
            case DISPLAY_COLOR:
            case DISPLAY_PRETTY:
            case DISPLAY_PROGRESS:
            case DISPLAY_HTTP:
                config = config.set(key, asBoolean(value));
                break;

            default:
                throw new RequestFailedException(undefinedConfigKey(key));
        }
        save();
    }

    private static String checkGroup(String value) {
        try {
            InetAddress address = Inet4Address.getByName(value);
            if (!address.isMulticastAddress()) {
                throw new RequestFailedException("Expected a valid multicast IP address");
            }
            return value;

        } catch (UnknownHostException e) {
            throw new RequestFailedException(e);
        }
    }

    private static String checkInterval(String value, int min, int max) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < min || parsed > max) {
                String msg = String.format("Expected an integer value in the range %s to %s, inclusive", min, max);
                throw new RequestFailedException(msg);
            }
            return value;

        } catch (NumberFormatException e) {
            throw new RequestFailedException(e);
        }
    }

    private static String checkDuration(String value) {
        if (!ConfigUtil.isValidDuration(value)) {
            throw new RequestFailedException("Expected a valid duration");
        }
        return value;
    }

    private static String checkFormat(String value) {
        if (!Format.isSupported(value)) {
            throw new RequestFailedException("Expected " + Joiner.on('|').join(Format.values()));
        }
        return value;
    }

    private static boolean asBoolean(String value) {
        switch (value.toLowerCase()) {
            case "true":
            case "on":
            case "yes":
                return true;

            case "false":
            case "off":
            case "no":
                return false;

            default:
                throw new RequestFailedException("Expected a boolean value");
        }
    }

    /**
     * Unset a config key.
     *
     * @param key Key.
     */
    public void unset(String key) {
        if (!listKeys().contains(key)) {
            throw new RequestFailedException(undefinedConfigKey(key));
        }
        config = config.unset(key);
        save();
    }

    /**
     * Reset config to its default values.
     */
    public void reset() {
        try {
            Files.deleteIfExists(CONFIG_PATH);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
        config = Config.empty();
        extended = DEFAULT.extend(config);
    }

    private void save() {
        extended = DEFAULT.extend(config);
        if (config.isEmpty()) {
            return;
        }
        try {
            Files.createDirectories(home());

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
        ConfigReadWrite.write(CONFIG_PATH, config);
    }

    private static String undefinedConfigKey(String key) {
        return "Undefined config key '" + key + "'";
    }
}
