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
package org.elasticlib.common.config;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.util.Arrays.asList;
import java.util.LinkedHashMap;
import java.util.Map;
import static org.elasticlib.common.config.Config.key;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.value.Value;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Units tests.
 */
public class ConfigTest {

    private static final String WEB = "web";
    private static final String SCHEME = "scheme";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String CONTEXT = "context";
    private static final String LOG = "log";
    private Config config;
    private Map<String, Value> webValue;
    private Map<String, Value> rootValue;

    /**
     * Initialisation.
     */
    @BeforeClass
    public void init() {
        config = Config.empty()
                .set(key(WEB, SCHEME), "http")
                .set(key(WEB, HOST), "127.0.0.1")
                .set(key(WEB, PORT), 8080)
                .set(key(LOG), true)
                .extend(readFromClassPath("config.yml"));

        webValue = new MapBuilder()
                .put(SCHEME, "http")
                .put(HOST, "localhost")
                .put(PORT, 80)
                .put(CONTEXT, "ctx")
                .build();

        rootValue = new MapBuilder()
                .put(WEB, webValue)
                .put(LOG, true)
                .build();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = ConfigException.class)
    public void readGarbageTest() {
        ConfigReadWrite.read(path("garbage.properties"));
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = ConfigException.class)
    public void readGarbageFromClassPathTest() {
        readFromClassPath("garbage.properties");
    }

    /**
     * Test.
     */
    @Test
    public void readEmptyFileTest() {
        assertThat(ConfigReadWrite.read(path("empty.yml")).isEmpty()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void readEmptyFileFromClassPathTest() {
        assertThat(readFromClassPath("empty.yml").isEmpty()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void readNonExistingFileTest() {
        Path path = path("config.yml")
                .resolve("../absent.yml")
                .normalize();

        assertThat(ConfigReadWrite.read(path).isEmpty()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void readNonExistingFileFromClassPathTest() {
        assertThat(readFromClassPath("absent.yml").isEmpty()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void isEmptyTest() {
        assertThat(Config.empty().isEmpty()).isTrue();
        assertThat(new Config(Value.ofNull()).isEmpty()).isFalse();
        assertThat(Config.empty().set("root", "val").isEmpty()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void listKeysEmptyTest() {
        assertThat(Config.empty().listKeys()).isEmpty();
    }

    /**
     * Test.
     */
    @Test
    public void listKeysTest() {
        assertThat(config.listKeys()).isEqualTo(asList(key(WEB, SCHEME),
                                                       key(WEB, HOST),
                                                       key(WEB, PORT),
                                                       key(WEB, CONTEXT),
                                                       key(LOG)));
    }

    /**
     * Test.
     */
    @Test
    public void containsKeyTest() {
        assertThat(config.containsKey(WEB)).isTrue();
        assertThat(config.containsKey(key(WEB, HOST))).isTrue();
        assertThat(config.containsKey("unknown")).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void getFlatMapTest() {
        Map<String, Value> expected = new MapBuilder()
                .put("je.maxMemory", 524288000)
                .put("je.lock.timeout", "5 m")
                .build();

        Config conf = Config.empty()
                .set("test", 42)
                .set("je.maxMemory", 524288000)
                .set("je.lock.timeout", "5 m");

        assertThat(conf.getFlatMap("je")).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void getTest() {
        assertThat(config.getString(key(WEB, SCHEME))).isEqualTo("http");
        assertThat(config.getString(key(WEB, HOST))).isEqualTo("localhost");
        assertThat(config.getInt(key(WEB, PORT))).isEqualTo(80);
        assertThat(config.getString(key(WEB, CONTEXT))).isEqualTo("ctx");
        assertThat(config.getBoolean(LOG)).isEqualTo(true);
    }

    /**
     * Test.
     */
    @Test
    public void getMapTest() {
        assertThat(config.get(WEB).asMap()).isEqualTo(webValue);
    }

    /**
     * Test.
     */
    @Test
    public void asValueTest() {
        assertThat(config.asValue().asMap()).isEqualTo(rootValue);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = ConfigException.class)
    public void getUndefinedTest() {
        config.get("absent");
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = ConfigException.class)
    public void getUnexpectedTypeTest() {
        config.getBoolean(key(WEB, HOST));
    }

    /**
     * Test.
     */
    @Test
    public void setTest() {
        Config updated = config.set(key(WEB, PORT), 443);
        assertThat(updated.getInt(key(WEB, PORT))).isEqualTo(443);
    }

    /**
     * Test.
     */
    @Test
    public void setRootTest() {
        Config updated = config.set("", Value.of("test"));
        assertThat(updated.asValue().asString()).isEqualTo("test");
    }

    /**
     * Test.
     */
    @Test
    public void unsetTest() {
        Map<String, Value> actual = config.unset(key(WEB, CONTEXT)).get(WEB).asMap();
        Map<String, Value> expected = new LinkedHashMap<>(webValue);
        expected.remove(CONTEXT);

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void unsetMapTest() {
        Map<String, Value> actual = config.unset(key(WEB)).asValue().asMap();
        Map<String, Value> expected = new LinkedHashMap<>(rootValue);
        expected.remove(WEB);

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void unsetRecursivelyTest() {
        Map<String, Value> actual = Config.empty()
                .set(key(LOG), true)
                .set(key(WEB, SCHEME), "http")
                .unset(key(WEB, SCHEME))
                .asValue()
                .asMap();

        Map<String, Value> expected = new LinkedHashMap<>(rootValue);
        expected.remove(WEB);

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test.
     */
    @Test
    public void unsetUndefinedTest() {
        assertThat(config.unset(key(WEB, "absent"))).isEqualTo(config);
    }

    /**
     * Test.
     */
    @Test
    public void unsetRootTest() {
        assertThat(config.unset("").isEmpty()).isTrue();
    }

    /**
     * Test.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void writeTest() throws IOException {
        Path path = Files.createTempFile("config", "yml");
        try {
            ConfigReadWrite.write(path, config);
            assertThat(ConfigReadWrite.read(path)).isEqualTo(config);

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private Config readFromClassPath(String fileName) {
        return ConfigReadWrite.readFromClassPath(getClass(), fileName);
    }

    private Path path(String fileName) {
        try {
            String resource = getClass().getPackage().getName().replace(".", "/") + "/" + fileName;
            return Paths.get(getClass().getClassLoader().getResource(resource).toURI());

        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }
}
