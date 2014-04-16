package store.common.config;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.MapBuilder;
import static store.common.config.Config.key;
import store.common.value.Value;

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
        config = new Config()
                .set(key(WEB, SCHEME), "http")
                .set(key(WEB, HOST), "127.0.0.1")
                .set(key(WEB, PORT), 8080)
                .set(key(LOG), true)
                .extend(ConfigReadWrite.read(path("config.yml")).get());

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
    @Test
    public void readEmptyFileTest() {
        assertThat(ConfigReadWrite.read(path("empty.yml")).isPresent()).isFalse();
    }

    /**
     * Test.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void readNonExistingFileTest() throws IOException {
        assertThat(ConfigReadWrite.read(path("empty.yml").resolve("../absent.yml")).isPresent()).isFalse();
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
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void writeTest() throws IOException {
        Path path = Files.createTempFile("config", "yml");
        try {
            ConfigReadWrite.write(path, config);
            Config loaded = ConfigReadWrite.read(path).get();

            assertThat(loaded).isEqualTo(config);

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private Path path(String filename) {
        try {
            String resource = getClass().getPackage().getName().replace(".", "/") + "/" + filename;
            return Paths.get(getClass().getClassLoader().getResource(resource).toURI());

        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }
}
